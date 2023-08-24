package bloblang

import (
	"fmt"
	bl "github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/mitchellh/hashstructure/v2"
)

func init() {
	err := bl.RegisterMethodV2("hash_any", bl.NewPluginSpec(), func(args *bl.ParsedParams) (bl.Method, error) {
		return func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			h, err := hashstructure.Hash(v, hashstructure.FormatV2, nil)
			if err != nil {
				return nil, err
			}

			return fmt.Sprintf("%d", h), nil
		}, nil
	})

	if err != nil {
		panic(err)
	}
}
