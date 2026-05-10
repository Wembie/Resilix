package config

import (
	"context"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"github.com/fsnotify/fsnotify"
)

func Watch(ctx context.Context, path, envPrefix string, overrides map[string]any, onChange func(resilix.Options)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			_ = watcher.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					options, loadErr := LoadOptions(path, envPrefix, overrides)
					if loadErr == nil && onChange != nil {
						onChange(options)
					}
				}
			case <-watcher.Errors:
			}
		}
	}()

	return watcher.Add(path)
}
