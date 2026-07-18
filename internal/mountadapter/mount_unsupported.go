//go:build !linux

package mountadapter

import (
	"context"

	"github.com/mydearniko/idoud/internal/mountcore"
	"github.com/mydearniko/idoud/internal/mountremote"
)

func mountPlatform(context.Context, *mountcore.Core, *mountremote.Backend, Options) (Session, error) {
	return nil, ErrUnsupported
}
