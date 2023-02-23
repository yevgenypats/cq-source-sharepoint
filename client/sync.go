package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cloudquery/plugin-sdk/plugins/source"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/thoas/go-funk"
)

func (c *Client) Sync(ctx context.Context, _ *source.Metrics, res chan<- *schema.Resource) error {
	for _, table := range c.Tables {
		meta := c.tablesMap[table.Name]
		if err := c.syncTable(ctx, res, table, meta); err != nil {
			return fmt.Errorf("syncing table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (c *Client) syncTable(ctx context.Context, res chan<- *schema.Resource, table *schema.Table, meta tableMeta) error {
	logger := c.Logger.With().Str("table", table.Name).Logger()

	list := c.SP.Web().GetList("Lists/" + meta.Title)
	items, err := list.Items().GetPaged()

	for {
		if err != nil {
			if IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get items: %w", err)
		}

		var itemList []map[string]any
		if err := json.Unmarshal(items.Items.Normalized(), &itemList); err != nil {
			return err
		}

		for _, itemMap := range itemList {
			//itemMap := item.ToMap()
			//b, _ := json.Marshal(itemMap)
			//fmt.Println(string(b))
			ks := funk.Keys(itemMap).([]string)
			sort.Strings(ks)
			logger.Debug().Strs("keys", ks).Msg("item keys")

			colVals := make([]any, len(table.Columns))
			var notFoundCols []string

			for i, col := range table.Columns {
				spName := meta.ColumnMap[col.Name]
				val, ok := itemMap[spName]
				if !ok {
					notFoundCols = append(notFoundCols, spName)
					colVals[i] = nil
					continue
				}
				colVals[i] = val
				delete(itemMap, spName)
			}

			if len(notFoundCols) > 0 {
				sort.Strings(notFoundCols)
				logger.Warn().Strs("missing_columns", notFoundCols).Msg("missing columns in result")
			}
			if len(itemMap) > 0 {
				ks := funk.Keys(itemMap).([]string)
				sort.Strings(ks)
				logger.Warn().Strs("extra_columns", ks).Msg("extra columns found in result")
			}

			resource, err := c.resourceFromValues(table.Name, colVals)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case res <- resource:
			}
		}

		if !items.HasNextPage() {
			break
		}
		items, err = items.GetNextPage()
	}

	return nil
}

func (c *Client) resourceFromValues(tableName string, values []any) (*schema.Resource, error) {
	table := c.Tables.Get(tableName)
	resource := schema.NewResourceData(table, nil, values)
	for i, col := range table.Columns {
		if err := resource.Set(col.Name, values[i]); err != nil {
			return nil, err
		}
	}
	return resource, nil
}
