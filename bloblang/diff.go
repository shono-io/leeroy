package bloblang

import (
	bl "github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/mitchellh/mapstructure"
	"github.com/r3labs/diff/v3"
)

func init() {
	err := bl.RegisterMethodV2("diff", bl.NewPluginSpec().Param(bl.NewAnyParam("other")), func(args *bl.ParsedParams) (bl.Method, error) {
		other, err := args.Get("other")
		if err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			cl, err := diff.Diff(v, other)
			if err != nil {
				return nil, err
			}

			var result []map[string]any
			if err := mapstructure.Decode(cl, &result); err != nil {
				return nil, err
			}

			return result, nil
		}, nil
	})

	if err != nil {
		panic(err)
	}
}
