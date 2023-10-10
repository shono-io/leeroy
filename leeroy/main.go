package main

import (
	"context"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
	"github.com/benthosdev/benthos/v4/public/service"
	_ "github.com/shono-io/leeroy/leeroy/components/elasticsearch"
	_ "github.com/shono-io/leeroy/leeroy/components/event"
	_ "github.com/shono-io/leeroy/leeroy/components/fork"
	_ "github.com/shono-io/leeroy/leeroy/components/gdrive/sheets"
	_ "github.com/shono-io/leeroy/leeroy/components/google"
	_ "github.com/shono-io/leeroy/leeroy/components/publish_kafka"
	_ "github.com/shono-io/leeroy/leeroy/components/reactor"
	_ "github.com/shono-io/leeroy/leeroy/components/salesforce"
	_ "github.com/shono-io/leeroy/leeroy/components/storage"
)

func main() {
	service.RunCLI(context.Background())
}
