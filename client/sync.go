package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cloudquery/plugin-sdk/plugins/source"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/google/uuid"
	"github.com/thoas/go-funk"
)

func (c *Client) Sync(ctx context.Context, metrics *source.Metrics, res chan<- *schema.Resource) error {
	for _, table := range c.Tables {
		if metrics.TableClient[table.Name] == nil {
			metrics.TableClient[table.Name] = make(map[string]*source.TableClientMetrics)
			metrics.TableClient[table.Name][c.ID()] = &source.TableClientMetrics{}
		}
	}

	for _, table := range c.Tables {
		meta := c.tablesMap[table.Name]
		m := metrics.TableClient[table.Name][c.ID()]
		if err := c.syncTable(ctx, m, res, table, meta); err != nil {
			return fmt.Errorf("syncing table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (c *Client) syncTable(ctx context.Context, metrics *source.TableClientMetrics, res chan<- *schema.Resource, table *schema.Table, meta tableMeta) error {
	logger := c.Logger.With().Str("table", table.Name).Logger()

	list := c.SP.Web().GetList("Lists/" + meta.Title)
	items, err := list.Items().GetPaged()

	for {
		if err != nil {
			if IsNotFound(err) {
				return nil
			}
			metrics.Errors++
			return fmt.Errorf("failed to get items: %w", err)
		}

		var itemList []map[string]any
		if err := json.Unmarshal(items.Items.Normalized(), &itemList); err != nil {
			metrics.Errors++
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
				if col.Name == pkField { // _cq_id currently has issues with unmanaged tables
					colVals[i] = uuid.New().String()
					continue
				}

				colMeta := meta.ColumnMap[col.Name]
				val, ok := itemMap[colMeta.SharepointName]
				if !ok {
					notFoundCols = append(notFoundCols, colMeta.SharepointName)
					colVals[i] = nil
					continue
				}
				colVals[i] = convertSharepointType(colMeta, val)
				delete(itemMap, colMeta.SharepointName)
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

			resource, err := resourceFromValues(table, colVals)
			if err != nil {
				metrics.Errors++
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case res <- resource:
				metrics.Resources++
			}
		}

		if !items.HasNextPage() {
			break
		}
		items, err = items.GetNextPage()
	}

	return nil
}

func resourceFromValues(table *schema.Table, values []any) (*schema.Resource, error) {
	resource := schema.NewResourceData(table, nil, values)
	for i, col := range table.Columns {
		if err := resource.Set(col.Name, values[i]); err != nil {
			return nil, err
		}
	}
	return resource, nil
}

func convertSharepointType(colMeta columnMeta, val any) any {
	switch colMeta.SharepointType {
	case "Currency":
		return fmt.Sprintf("%f", val)
	default:
		return val
	}
}
