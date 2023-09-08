package bloblang

import (
	bl "github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/mitchellh/mapstructure"
	"github.com/r3labs/diff/v3"
	"go.uber.org/multierr"
)

func init() {
	err := bl.RegisterMethodV2("patch", bl.NewPluginSpec().Param(bl.NewAnyParam("changelog")), func(args *bl.ParsedParams) (bl.Method, error) {
		clog, err := args.Get("changelog")
		if err != nil {
			return nil, err
		}

		var cl diff.Changelog
		if err := mapstructure.Decode(clog, &cl); err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			pl := diff.Patch(cl, &v)

			if pl.HasErrors() {
				var e error
				for _, ple := range pl {
					if ple.Errors != nil {
						if err := multierr.Append(e, ple.Errors); err != nil {
							return nil, err
						}
					}
				}

				return nil, e
			}

			return v, nil
		}, nil
	})

	if err != nil {
		panic(err)
	}
}
