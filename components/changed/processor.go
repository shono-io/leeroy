package sheets

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/mitchellh/hashstructure/v2"
	"go.uber.org/multierr"
)

func init() {
	if err := service.RegisterProcessor("changed", config(), newProc); err != nil {
		panic(err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("cache")).
		Field(service.NewInterpolatedStringField("key")).
		Field(service.NewProcessorListField("when_new").Default(nil)).
		Field(service.NewProcessorListField("when_changed").Default(nil)).
		Field(service.NewProcessorListField("when_unchanged").Default(nil))
}

func newProc(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	cache, err := conf.FieldString("cache")
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldInterpolatedString("key")
	if err != nil {
		return nil, err
	}

	whenNew, err := conf.FieldProcessorList("when_new")
	if err != nil {
		return nil, err
	}

	whenChanged, err := conf.FieldProcessorList("when_changed")
	if err != nil {
		return nil, err
	}

	whenUnchanged, err := conf.FieldProcessorList("when_unchanged")
	if err != nil {
		return nil, err
	}

	return &proc{
		cache:         cache,
		key:           key,
		whenNew:       whenNew,
		whenChanged:   whenChanged,
		whenUnchanged: whenUnchanged,
		mgr:           mgr,
	}, nil
}

type proc struct {
	cache         string
	key           *service.InterpolatedString
	whenNew       []*service.OwnedProcessor
	whenChanged   []*service.OwnedProcessor
	whenUnchanged []*service.OwnedProcessor
	mgr           *service.Resources
}

func (p *proc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	if message == nil {
		return nil, nil
	}

	// -- hash the message payload
	payload, err := message.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal message payload: %w", err)
	}

	ph, err := hashstructure.Hash(payload, hashstructure.FormatV2, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to hash message payload: %w", err)
	}
	messageHash := fmt.Sprintf("%d", ph)

	// -- get the key
	key, err := p.key.TryString(message)
	if err != nil {
		return nil, fmt.Errorf("unable to extract key: %w", err)
	}

	// -- get the item from the cache
	hash, fnd, err := p.getHashFromCache(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("unable to access cache: %w", err)
	}

	// -- compare the hashes
	changed := fnd && hash != messageHash

	// -- execute the outcomes
	if err := p.processOutcome(ctx, fnd, changed, message); err != nil {
		return nil, fmt.Errorf("unable to process outcomes: %w", err)
	}

	// -- formulate the result message
	result := service.NewMessage(nil)
	result.SetStructuredMut(map[string]any{
		"changed": changed,
		"found":   fnd,
		"hashes": map[string]string{
			"message": messageHash,
			"cache":   hash,
		},
	})

	return service.MessageBatch{result}, nil
}

func (p *proc) processOutcome(ctx context.Context, found, changed bool, original *service.Message) error {
	var procs []*service.OwnedProcessor
	if !found {
		procs = p.whenNew
	} else if changed {
		procs = p.whenChanged
	} else {
		procs = p.whenUnchanged
	}

	if procs == nil {
		return nil
	}

	res, err := service.ExecuteProcessors(ctx, procs, service.MessageBatch{original})
	if err != nil {
		return fmt.Errorf("unable to execute processors: %w", err)
	}

	var errs error
	for _, m := range res {
		for _, v := range m {
			if v != nil {
				if err := v.GetError(); err != nil {
					errs = multierr.Append(errs, err)
				}
			}
		}
	}

	return errs
}

func (p *proc) Close(ctx context.Context) error {
	return nil
}

func (p *proc) getHashFromCache(ctx context.Context, key string) (string, bool, error) {
	fnd := false
	var result map[string]any
	var ierr error

	err := p.mgr.AccessCache(ctx, p.cache, func(cache service.Cache) {
		res, err := cache.Get(ctx, key)
		if err != nil {
			if err == service.ErrKeyNotFound {
				return
			}

			ierr = err
			return
		}

		fnd = true
		ierr = json.Unmarshal(res, &result)
	})
	if err != nil {
		return "", false, fmt.Errorf("unable to access cache: %w", err)
	}

	if fnd {
		h, err := hashstructure.Hash(result, hashstructure.FormatV2, nil)
		if err != nil {
			ierr = fmt.Errorf("unable to hash result: %w", err)
		}

		return fmt.Sprintf("%d", h), fnd, ierr
	}

	return "", fnd, ierr
}
