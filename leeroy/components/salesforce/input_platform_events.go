package salesforce

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"time"
)

func init() {
	if err := service.RegisterBatchInput("salesforce_platform_events", config(), newInput); err != nil {
		panic(err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("topic")).
		Field(service.NewObjectField("auth",
			service.NewStringField("endpoint"),
			service.NewStringField("username"),
			service.NewStringField("password"),
			service.NewStringField("client_id"),
			service.NewStringField("client_secret"),
		)).
		Field(service.NewStringField("grpc_endpoint").Default("api.pubsub.salesforce.com:7443")).
		Field(service.NewIntField("batchSize").Default(50)).
		Field(service.NewDurationField("timeout").Default("5s")).
		Field(service.NewStringField("replay_preset").Default("earliest"))
}

func newInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	topic, err := conf.FieldString("topic")
	if err != nil {
		return nil, err
	}

	authEndpoint, err := conf.FieldString("auth", "endpoint")
	if err != nil {
		return nil, err
	}

	authUsername, err := conf.FieldString("auth", "username")
	if err != nil {
		return nil, err
	}

	authPassword, err := conf.FieldString("auth", "password")
	if err != nil {
		return nil, err
	}

	authClientId, err := conf.FieldString("auth", "client_id")
	if err != nil {
		return nil, err
	}

	authClientSecret, err := conf.FieldString("auth", "client_secret")
	if err != nil {
		return nil, err
	}

	grpcEndpoint, err := conf.FieldString("grpc_endpoint")
	if err != nil {
		return nil, err
	}

	batchSize, err := conf.FieldInt("batchSize")
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration("timeout")
	if err != nil {
		return nil, err
	}

	replayPresetStr, err := conf.FieldString("replay_preset")
	if err != nil {
		return nil, err
	}

	replayPreset, err := parseReplayPreset(replayPresetStr)
	if err != nil {
		return nil, err
	}

	return &input{
		grpcEndpoint: grpcEndpoint,
		topic:        topic,
		batchSize:    int32(batchSize),
		authEndpoint: authEndpoint,
		clientId:     authClientId,
		clientSecret: authClientSecret,
		username:     authUsername,
		password:     authPassword,
		timout:       timeout,
		replayPreset: replayPreset,

		logger: mgr.Logger(),
	}, nil
}

func parseReplayPreset(str string) (ReplayPreset, error) {
	switch str {
	case "earliest":
		return ReplayPreset_EARLIEST, nil
	case "latest":
		return ReplayPreset_LATEST, nil
	default:
		return ReplayPreset_EARLIEST, fmt.Errorf("unsupported replay preset: %s", str)
	}
}

type input struct {
	grpcEndpoint string
	topic        string
	batchSize    int32
	authEndpoint string
	clientId     string
	clientSecret string
	username     string
	password     string
	timout       time.Duration
	replayPreset ReplayPreset

	logger *service.Logger

	client *PubSubClientWrapper
	sub    *Subscription
}

func (i *input) Connect(ctx context.Context) error {
	lcfg := LoginConfig{
		ClientId:     i.clientId,
		ClientSecret: i.clientSecret,
		Username:     i.username,
		Password:     i.password,
		Endpoint:     i.authEndpoint,
	}
	client, err := NewGRPCClient(i.grpcEndpoint, lcfg, i.timout, i.logger)
	if err != nil {
		return err
	}

	i.client = client

	err = client.FetchUserInfo(i.timout)
	if err != nil {
		client.Close()
		return fmt.Errorf("could not fetch user info: %v", err)
	}

	topic, err := client.GetTopic(i.topic, i.timout)
	if err != nil {
		client.Close()
		return fmt.Errorf("could not fetch topic: %v", err)
	}

	if !topic.GetCanSubscribe() {
		client.Close()
		return fmt.Errorf("this user is not allowed to subscribe to the following topic: %s", i.topic)
	}

	sub, err := client.Subscribe(i.topic, i.batchSize, i.replayPreset)
	if err != nil {
		client.Close()
		return fmt.Errorf("could not subscribe to topic: %v", err)
	}
	i.sub = sub

	return nil
}

func (i *input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	// -- read the batch from the subscription
	result, err := i.sub.ReadBatch()
	if err != nil {
		return nil, nil, err
	}

	return result, func(ctx context.Context, err error) error {
		if err == nil {
			return i.sub.Commit()
		}

		return err
	}, nil
}

func (i *input) Close(ctx context.Context) error {
	if i.client != nil {
		i.client.Close()
	}

	return nil
}
