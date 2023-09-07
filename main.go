package main

import (
	"context"
	_ "github.com/benthosdev/benthos/v4/public/components/confluent"
	_ "github.com/benthosdev/benthos/v4/public/components/elasticsearch"
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/pure/extended"
	_ "github.com/benthosdev/benthos/v4/public/components/redis"
	"github.com/benthosdev/benthos/v4/public/service"
	_ "github.com/shono-io/leeroy/components/elasticsearch"
	_ "github.com/shono-io/leeroy/components/event"
	_ "github.com/shono-io/leeroy/components/fork"
	_ "github.com/shono-io/leeroy/components/gdrive/sheets"
	_ "github.com/shono-io/leeroy/components/publish_kafka"
	_ "github.com/shono-io/leeroy/components/reactor"
	_ "github.com/shono-io/leeroy/components/storage"
)

func main() {
	service.RunCLI(context.Background())
}
