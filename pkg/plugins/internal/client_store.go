package internal

import (
	"sync"

	"github.com/hashicorp/go-plugin"
)

type clientProps struct {
	ID   PluginIdentity
	Auth PluginHandshake
	P    plugin.Plugin
}

type Client struct {
	PluginClient *plugin.Client
	ClientProps  *clientProps
}

// clientStore maps the available plugin names to the different kinds of plugin.Client plugin controllers and their properties.
//
// The map might include a mapping of "delta" -> `{ plugin.Client, "deltaPluginLocation", ["arg1"], ["env1"] }` to
// communicate with the Delta plugin.
type clientStore struct {
	pluginApplicationClients map[string]*Client
	rwl                      sync.RWMutex
}

func newClientsMap() *clientStore {
	return &clientStore{
		pluginApplicationClients: make(map[string]*Client),
	}
}

func (cs *clientStore) Insert(name string, c *plugin.Client, cp *clientProps) {
	cs.rwl.Lock()
	defer cs.rwl.Unlock()
	cl := &Client{
		c,
		cp,
	}
	cs.pluginApplicationClients[name] = cl
}

func (cs *clientStore) Remove(name string) {
	cs.rwl.Lock()
	defer cs.rwl.Unlock()
	delete(cs.pluginApplicationClients, name)
}

func (cs *clientStore) Client(name string) (*plugin.Client, *clientProps, error) {
	cs.rwl.RLock()
	defer cs.rwl.RUnlock()
	cl, ok := cs.pluginApplicationClients[name]
	if !ok {
		return nil, nil, ErrPluginNotFound
	}
	return cl.PluginClient, cl.ClientProps, nil
}
