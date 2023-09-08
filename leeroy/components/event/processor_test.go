package event

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestProcInit(t *testing.T) {
	fnd := false
	service.GlobalEnvironment().WalkProcessors(func(name string, config *service.ConfigView) {
		if name == "event" {
			fnd = true
		}
	})

	assert.True(t, fnd)
}

func TestProcess(t *testing.T) {
	t.Run("should add headers", shouldAddHeaders)
}

func shouldAddHeaders(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	conf, err := config().ParseYAML(strings.TrimSpace(`
scope: foo
concept: bar
event: baz
`), service.GlobalEnvironment())
	assert.NoError(t, err)

	prc, err := newProcessor(conf, nil)
	require.NoError(t, err)

	// when
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{
		"foo": "bar",
	})

	res, err := prc.Process(tCtx, msg)
	assert.NoError(t, err)
	assert.Len(t, res, 1)

	scope, fnd := res[0].MetaGet("scope")
	assert.True(t, fnd)
	assert.Equal(t, scope, "foo")

	concept, fnd := res[0].MetaGet("concept")
	assert.True(t, fnd)
	assert.Equal(t, concept, "bar")

	event, fnd := res[0].MetaGet("event")
	assert.True(t, fnd)
	assert.Equal(t, event, "baz")
}
