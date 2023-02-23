package client

import (
	"fmt"

	"github.com/thoas/go-funk"
)

type Spec struct {
	SiteURL      string `json:"site_url"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`

	Lists         []string            `json:"lists"`
	ListFields    map[string][]string `json:"list_fields"`
	DefaultFields []string            `json:"default_fields"`
	IgnoreFields  []string            `json:"ignore_fields"`
}

func (s *Spec) SetDefaults() {
	if s.ListFields == nil {
		s.ListFields = make(map[string][]string)
	}

	if len(s.DefaultFields) == 0 {
		s.DefaultFields = []string{
			"Id",
			"Created",
			"Modified",
			"Title",
			"AuthorId",
			"EditorId",
			"version",
			"FSObjType",
		}
	}

	if len(s.IgnoreFields) == 0 {
		s.IgnoreFields = []string{
			"__metadata",
		}
	}
}

func (s Spec) Validate() error {
	if s.ClientID == "" {
		return fmt.Errorf("client_id is required")
	}
	if s.ClientSecret == "" {
		return fmt.Errorf("client_secret is required")
	}
	if s.SiteURL == "" {
		return fmt.Errorf("site_url is required")
	}

	dupeLists := make(map[string]struct{}, len(s.Lists))
	for _, title := range s.Lists {
		name := normalizeName(title)
		if _, ok := dupeLists[name]; ok {
			return fmt.Errorf("found duplicate normalized list name in spec: %q (%q)", title, name)
		}
		dupeLists[name] = struct{}{}
	}

	if len(s.Lists) > 0 {
		for k := range s.ListFields {
			name := normalizeName(k)
			if _, ok := dupeLists[name]; !ok {
				return fmt.Errorf("found list_fields for unspecified list in spec: %q", k)
			}
		}
	}

	return nil
}

func (s Spec) ShouldSelectField(list, field string) bool {
	if funk.ContainsString(s.IgnoreFields, field) {
		return false
	}

	fields := s.ListFields[list]
	if len(fields) == 0 {
		// If no fields are specified for this list, use the default fields
		return funk.ContainsString(s.DefaultFields, field)
	}

	return funk.ContainsString(fields, field)
}
