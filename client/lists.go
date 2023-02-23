package client

import (
	"fmt"
	"strings"

	"github.com/cloudquery/plugin-sdk/caser"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/koltyakov/gosip/api"
	"github.com/rs/zerolog"
)

const pkField = "sharepoint_listrow_id"

func (c *Client) getAllLists() ([]string, error) {
	lists, err := c.SP.Web().Lists().Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get lists: %w", err)
	}

	listsData := lists.Data()

	listOfLists := make([]string, 0, len(listsData))
	normalizedNames := make(map[string]struct{})
	for _, list := range listsData {
		d := list.Data()
		name := normalizeName(d.Title)
		if _, ok := normalizedNames[name]; ok {
			c.Logger.Warn().Msgf("List %q has been normalized to %q, but another list has already been normalized to that name. skipping %q", d.Title, name, d.Title)
			continue
		}

		normalizedNames[name] = struct{}{}
		listOfLists = append(listOfLists, d.Title)
	}

	return listOfLists, nil
}

func (c *Client) tableFromList(title string) (*schema.Table, *tableMeta, error) {
	name := normalizeName(title)
	table := &schema.Table{
		Name:        "sharepoint_" + name,
		Description: title,
	}
	logger := c.Logger.With().Str("table", table.Name).Logger()

	ld := c.SP.Web().GetList("Lists/" + title)
	fields, err := ld.Fields().Get()
	if err != nil {
		if IsNotFound(err) { // Not found is ok, just skip this table
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get fields: %w", err)
	}

	//var itemList []map[string]any
	//if err := json.Unmarshal(fields.Normalized(), &itemList); err != nil {
	//	return nil, nil, err
	//}

	fieldsData := fields.Data()
	meta := &tableMeta{
		Title:     title,
		ColumnMap: make(map[string]columnMeta, len(fieldsData)),
	}

	table.Columns = append(table.Columns, schema.Column{
		Name:        pkField,
		Description: "The unique identifier of the list item.",
		Type:        schema.TypeUUID,
		CreationOptions: schema.ColumnCreationOptions{
			PrimaryKey: true,
		},
	})

	dupeColNames := make(map[string]int, len(fieldsData))
	for _, field := range fieldsData {
		fieldData := field.Data()

		col := columnFromField(fieldData, logger)
		if i := dupeColNames[col.Name]; i > 0 {
			dupeColNames[col.Name] = i + 1
			col.Name = fmt.Sprintf("%s_%d", col.Name, i)
		} else {
			dupeColNames[col.Name] = 1
		}

		table.Columns = append(table.Columns, col)
		meta.ColumnMap[col.Name] = columnMeta{
			SharepointName: fieldData.InternalName,
			SharepointType: fieldData.TypeAsString,
		}
	}
	return table, meta, nil
}

func columnFromField(field *api.FieldInfo, logger zerolog.Logger) schema.Column {
	c := schema.Column{
		Name:        normalizeName(field.InternalName),
		Description: field.Description,
	}
	switch field.TypeAsString {
	case "Text", "Note", "ContentTypeId":
		c.Type = schema.TypeString
	case "Integer", "Counter":
		c.Type = schema.TypeInt
	case "Currency":
		c.Type = schema.TypeString // We override this later to be able to represent Currency as strings
	case "Number":
		c.Type = schema.TypeFloat
	case "DateTime":
		c.Type = schema.TypeTimestamp
	case "Boolean":
		c.Type = schema.TypeBool
	case "Guid":
		c.Type = schema.TypeUUID
	case "Lookup":
		c.Type = schema.TypeIntArray
	case "Choice":
		c.Type = schema.TypeString
	case "MultiChoice":
		c.Type = schema.TypeStringArray
	case "User":
		c.Type = schema.TypeJSON
	case "Computed":
		c.Type = schema.TypeJSON
	default:
		logger.Warn().Str("type", field.TypeAsString).Int("kind", field.FieldTypeKind).Str("field_title", field.Title).Str("field_id", field.ID).Msg("unknown type, assuming JSON")
		c.Type = schema.TypeJSON
	}
	//logger.Info().Str("type", field.TypeAsString).Int("kind", field.FieldTypeKind).Str("field_title", field.Title).Str("field_id", field.ID).Any("f", field).Msg("found field")

	return c
}

func normalizeName(name string) string {
	csr := caser.New()
	s := csr.ToSnake(name)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}
