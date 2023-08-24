package reactor

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
		if name == "reactor" {
			fnd = true
		}
	})

	assert.True(t, fnd)
}

func TestProcess(t *testing.T) {
	t.Run("should process a matching event", processMatch)
	t.Run("should ignore a non-matching event", ignoreUnmatched)
}

func processMatch(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	conf, err := config().ParseYAML(strings.TrimSpace(`
events:
  - on:
      scope: foo
      concept: bar
      event: baz
    processors:
      - mapping: root = this
`), service.GlobalEnvironment())
	assert.NoError(t, err)

	if conf == nil {
		t.FailNow()
	}

	prc, err := newProcessor(conf, nil)
	require.NoError(t, err)

	// when
	msg := service.NewMessage(nil)
	msg.MetaSetMut("scope", "foo")
	msg.MetaSetMut("concept", "bar")
	msg.MetaSetMut("event", "baz")
	msg.SetStructuredMut(map[string]any{
		"foo": "bar",
	})

	res, err := prc.Process(tCtx, msg)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
}

func ignoreUnmatched(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	conf, err := config().ParseYAML(strings.TrimSpace(`
events:
  - on:
      scope: foo
      concept: bar
      event: baz
    processors:
      - mapping: root = this
`), service.GlobalEnvironment())
	assert.NoError(t, err)

	prc, err := newProcessor(conf, nil)
	require.NoError(t, err)

	// when
	msg := service.NewMessage(nil)
	msg.MetaSetMut("scope", "foo")
	msg.MetaSetMut("concept", "bar")
	msg.MetaSetMut("event", "zoo")
	msg.SetStructuredMut(map[string]any{
		"foo": "zoo",
	})

	res, err := prc.Process(tCtx, msg)
	assert.NoError(t, err)
	assert.Len(t, res, 0)
}
