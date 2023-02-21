package client

import "fmt"

type Spec struct {
	SiteURL      string   `json:"site_url"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	Lists        []string `json:"lists"`
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

	return nil
}
