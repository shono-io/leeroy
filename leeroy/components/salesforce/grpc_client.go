package salesforce

import (
	"context"
	"crypto/x509"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"io"
	"log"
	"time"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	tokenHeader    = "accesstoken"
	instanceHeader = "instanceurl"
	tenantHeader   = "tenantid"
)

type PubSubClientWrapper struct {
	accessToken string
	instanceURL string
	oauthURL    string

	userID string
	orgID  string

	conn         *grpc.ClientConn
	pubSubClient PubSubClient

	logger *service.Logger

	schemas Schemas
}

// Closes the underlying connection to the gRPC server
func (c *PubSubClientWrapper) Close() {
	c.conn.Close()
}

// Makes a call to the OAuth server to fetch user info. User info is stored as part of the PubSubClient object so that it can be referenced
// later in other methods
func (c *PubSubClientWrapper) FetchUserInfo(timeout time.Duration) error {
	resp, err := UserInfo(c.oauthURL, c.accessToken, timeout)
	if err != nil {
		return err
	}

	c.userID = resp.UserID
	c.orgID = resp.OrganizationID

	return nil
}

// Wrapper function around the GetTopic RPC. This will add the OAuth credentials and make a call to fetch data about a specific topic
func (c *PubSubClientWrapper) GetTopic(topicName string, timeout time.Duration) (*TopicInfo, error) {
	var trailer metadata.MD

	req := &TopicRequest{
		TopicName: topicName,
	}

	ctx, cancelFn := context.WithTimeout(c.getAuthContext(), timeout)
	defer cancelFn()

	resp, err := c.pubSubClient.GetTopic(ctx, req, grpc.Trailer(&trailer))
	printTrailer(trailer)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Wrapper function around the Publish RPC. This will add the OAuth credentials and produce a single hardcoded event to the specified topic.
func (c *PubSubClientWrapper) Publish(topicName string, schema *SchemaInfo, data map[string]any, timeout time.Duration) error {
	log.Printf("Creating codec from schema...")
	codec, err := goavro.NewCodec(schema.SchemaJson)
	if err != nil {
		return err
	}

	payload, err := codec.BinaryFromNative(nil, data)
	if err != nil {
		return err
	}

	var trailer metadata.MD

	req := &PublishRequest{
		TopicName: topicName,
		Events: []*ProducerEvent{
			{
				SchemaId: schema.GetSchemaId(),
				Payload:  payload,
			},
		},
	}

	ctx, cancelFn := context.WithTimeout(c.getAuthContext(), timeout)
	defer cancelFn()

	pubResp, err := c.pubSubClient.Publish(ctx, req, grpc.Trailer(&trailer))
	printTrailer(trailer)

	if err != nil {
		return err
	}

	result := pubResp.GetResults()
	if result == nil {
		return fmt.Errorf("nil result returned when publishing to %s", topicName)
	}

	if err := result[0].GetError(); err != nil {
		return fmt.Errorf(result[0].GetError().GetMsg())
	}

	return nil
}

// Returns a new context with the necessary authentication parameters for the gRPC server
func (c *PubSubClientWrapper) getAuthContext() context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		tokenHeader, c.accessToken,
		instanceHeader, c.instanceURL,
		tenantHeader, c.orgID,
	))
}

// Creates a new connection to the gRPC server and returns the wrapper struct
func NewGRPCClient(grpcEndpoint string, cfg LoginConfig, timeout time.Duration, logger *service.Logger) (*PubSubClientWrapper, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	if grpcEndpoint == "localhost:7011" {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		certs := getCerts()
		creds := credentials.NewClientTLSFromCert(certs, "")
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	conn, err := grpc.DialContext(ctx, grpcEndpoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	auth, err := Login(cfg, timeout)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("unable to authenticate: %w", err)
	}

	ui, err := UserInfo(cfg.Endpoint, auth.AccessToken, timeout)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve user info: %w", err)
	}

	result := &PubSubClientWrapper{
		conn:         conn,
		pubSubClient: NewPubSubClient(conn),
		accessToken:  auth.AccessToken,
		instanceURL:  auth.InstanceURL,
		userID:       ui.UserID,
		orgID:        ui.OrganizationID,
		oauthURL:     cfg.Endpoint,
		logger:       logger,
	}

	result.schemas = NewSchemas(result.pubSubClient, timeout, result.getAuthContext())

	return result, nil
}

// Fetches system certs and returns them if possible. If unable to fetch system certs then an empty cert pool is returned instead
func getCerts() *x509.CertPool {
	if certs, err := x509.SystemCertPool(); err == nil {
		return certs
	}

	return x509.NewCertPool()
}

// Helper function to display trailers on the console in a more readable format
func printTrailer(trailer metadata.MD) {
	if len(trailer) == 0 {
		log.Printf("no trailers returned")
		return
	}

	log.Printf("beginning of trailers")
	for key, val := range trailer {
		log.Printf("[trailer] = %s, [value] = %s", key, val)
	}
	log.Printf("end of trailers")
}

func (c *PubSubClientWrapper) Subscribe(topic string, batchSize int32, preset ReplayPreset) (*Subscription, error) {
	subscribeClient, err := c.pubSubClient.Subscribe(c.getAuthContext())
	if err != nil {
		return nil, err
	}

	return &Subscription{
		logger:       c.logger,
		sc:           subscribeClient,
		schemas:      c.schemas,
		topic:        topic,
		batchSize:    batchSize,
		replayPreset: preset,
		replayId:     &MemoryReplayState{},
	}, nil
}

type Subscription struct {
	sc           PubSub_SubscribeClient
	schemas      Schemas
	topic        string
	batchSize    int32
	replayPreset ReplayPreset

	logger *service.Logger

	replayId            ReplayState
	unconfirmedReplayId []byte
}

func (sub *Subscription) CurrentReplayId() []byte {
	return sub.replayId.Get()
}

func (sub *Subscription) IsBatchPending() bool {
	return sub.unconfirmedReplayId != nil
}

func (sub *Subscription) ReadBatch() (service.MessageBatch, error) {
	if sub.IsBatchPending() {
		return nil, fmt.Errorf("cannot read a new batch until the previous batch has been confirmed")
	}

	result := service.MessageBatch{}

	// send a request to the server to fetch events
	fetchRequest := &FetchRequest{
		TopicName:    sub.topic,
		NumRequested: sub.batchSize,
		ReplayId:     sub.replayId.Get(),
		ReplayPreset: sub.replayPreset,
	}
	if sub.replayPreset == ReplayPreset_CUSTOM && sub.replayId.Get() != nil {
		fetchRequest.ReplayId = sub.replayId.Get()
	}

	if err := sub.sc.Send(fetchRequest); err != nil {
		if err == io.EOF {
			sub.logger.Debugf("No new events to fetch\n")
			//return nil, nil
		}

		return nil, err
	}

	// start processing all the events we expect to receive
	resp, err := sub.sc.Recv()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}

		return nil, err
	}

	for _, event := range resp.Events {
		codec, err := sub.schemas.Fetch(event.GetEvent().GetSchemaId())
		if err != nil {
			return nil, err
		}

		parsed, _, err := codec.NativeFromBinary(event.GetEvent().GetPayload())
		if err != nil {
			return nil, err
		}

		body, ok := parsed.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("error casting parsed event: %v", body)
		}

		// store the replay id so that we can commit it later
		sub.unconfirmedReplayId = event.ReplayId

		msg := service.NewMessage(nil)
		msg.SetStructured(body)
		msg.MetaSetMut("sf_replay_id", event.ReplayId)
		msg.MetaSetMut("sf_schema_id", event.Event.SchemaId)
		for _, h := range event.Event.Headers {
			msg.MetaSetMut(h.Key, h.Value)
		}

		result = append(result, msg)
	}

	return result, nil
}

func (sub *Subscription) Commit() error {
	if !sub.IsBatchPending() {
		return nil
	}

	if err := sub.replayId.Set(sub.unconfirmedReplayId); err != nil {
		return fmt.Errorf("unable to store the replay id: %w", err)
	}

	sub.unconfirmedReplayId = nil
	return nil
}

func (sub *Subscription) Close() error {
	return sub.sc.CloseSend()
}
