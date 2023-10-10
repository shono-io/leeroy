package google

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
	"os"
	"regexp"
)

func init() {
	if err := service.RegisterBatchInput("gmail_messages", config(), newInput); err != nil {
		panic(err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("userId")).
		Field(service.NewStringField("query")).
		Field(service.NewStringField("attachmentRegex").Default(".*"))
}

func newInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	userId, err := conf.FieldString("userId")
	if err != nil {
		return nil, err
	}

	query, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	ar, err := conf.FieldString("attachmentRegex")
	if err != nil {
		return nil, err
	}

	arg, err := regexp.Compile(ar)
	if err != nil {
		return nil, fmt.Errorf("invalid attachment regex: %w", err)
	}

	return &input{
		userId:          userId,
		query:           query,
		attachmentRegex: arg,
		logger:          mgr.Logger(),
	}, nil
}

type input struct {
	userId string
	query  string

	attachmentRegex *regexp.Regexp

	service *gmail.Service
	logger  *service.Logger

	nextPageToken string
	end           bool
}

func (i *input) Connect(ctx context.Context) error {
	b, err := os.ReadFile("credentials.json")
	if err != nil {
		return fmt.Errorf("unable to read client secret file %q: %v", "credentials.json", err)
	}

	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
	if err != nil {
		return fmt.Errorf("unable to parse client secret file to config: %v", err)
	}

	tok, err := tokenFromFile("token.json")

	gmailService, err := gmail.NewService(context.Background(), option.WithHTTPClient(config.Client(context.Background(), tok)))
	if err != nil {
		return err
	}

	i.service = gmailService

	return nil
}

func (i *input) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	//i.logger.Infof("Listing messages for user %s matching %q and token %v", i.userId, i.query, i.nextPageToken)

	// construct the request
	call := i.service.Users.Messages.List(i.userId).Q(i.query).IncludeSpamTrash(false)

	if i.nextPageToken != "" {
		call = call.PageToken(i.nextPageToken)
	} else {
		if i.end {
			return nil, nil, service.ErrEndOfInput
		}
	}

	r, err := call.Do()
	if err != nil {
		return nil, nil, err
	}

	// -- run through the list of messages, and for each one, get the message
	msgBatch := service.MessageBatch{}
	if !i.end {
		for _, m := range r.Messages {
			msg, err := i.service.Users.Messages.Get(i.userId, m.Id).Do()
			if err != nil {
				return nil, nil, err
			}

			b, err := msg.MarshalJSON()
			if err != nil {
				return nil, nil, err
			}

			var result map[string]any
			if err := json.Unmarshal(b, &result); err != nil {
				return nil, nil, err
			}

			res := service.NewMessage(nil)
			res.SetStructured(result)

			msgBatch = append(msgBatch, res)
		}

		i.logger.Infof("retrieved %d messages", len(msgBatch))
	}

	return msgBatch, func(ctx context.Context, err error) error {
		if err == nil {
			i.nextPageToken = r.NextPageToken
			if i.nextPageToken == "" {
				i.end = true
			}
		}

		return err
	}, nil
}

func (i *input) Close(ctx context.Context) error {
	i.service = nil
	return nil
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}
