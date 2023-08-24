package sheets

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"google.golang.org/api/sheets/v4"
)

func init() {
	if err := service.RegisterProcessor("gdrive_sheets", config(), newProc); err != nil {
		panic(err)
	}
}

func config() *service.ConfigSpec {
	return service.NewConfigSpec().
		Field(service.NewStringField("spreadsheet_id")).
		Field(service.NewIntField("sheet_sequence").Default(0)).
		Field(service.NewIntField("header_offset").Default(0))
}

func newProc(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	sheetsService, err := sheets.NewService(context.Background())
	if err != nil {
		return nil, err
	}

	ssId, err := conf.FieldString("spreadsheet_id")
	if err != nil {
		return nil, err
	}

	sheetSeq, err := conf.FieldInt("sheet_sequence")
	if err != nil {
		return nil, err
	}
	if sheetSeq < 0 {
		return nil, fmt.Errorf("sheet_sequence must be >= 0")
	}

	headerOffset, err := conf.FieldInt("header_offset")
	if err != nil {
		return nil, err
	}
	if headerOffset < 0 {
		return nil, fmt.Errorf("header_offset must be >= 0")
	}

	return &proc{
		sheetsService: sheetsService,
		spreadsheetId: ssId,
		sheetSequence: sheetSeq,
	}, nil
}

type proc struct {
	sheetsService *sheets.Service
	spreadsheetId string
	sheetSequence int
	headerOffset  int
}

func (p *proc) Process(ctx context.Context, message *service.Message) (service.MessageBatch, error) {
	ss, err := p.sheetsService.Spreadsheets.Get(p.spreadsheetId).Do()
	if err != nil {
		return nil, err
	}

	if len(ss.Sheets) <= p.sheetSequence {
		return nil, fmt.Errorf("sheet_sequence %d is out of range", p.sheetSequence)
	}

	sheet := ss.Sheets[p.sheetSequence]
	if len(sheet.Data) == 0 {
		return nil, nil
	}

	var batch service.MessageBatch
	data := sheet.Data[0]
	columnNames := make([]string, len(data.RowData[0].Values))
	for ridx, row := range data.RowData {
		if ridx < p.headerOffset {
			continue
		}

		msg := make(map[string]interface{})
		for cidx, cell := range row.Values {
			if ridx == p.headerOffset {
				columnNames[cidx] = cell.FormattedValue
			} else {
				msg[columnNames[cidx]] = cell.EffectiveValue
			}
		}

		m := service.NewMessage(nil)
		m.SetStructuredMut(msg)
		m.MetaSetMut("sheet", sheet.Properties.Title)
		m.MetaSetMut("row", ridx)

		batch = append(batch, m)
	}

	return batch, nil
}

func (p *proc) Close(ctx context.Context) error {
	return nil
}
