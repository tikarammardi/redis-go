package store

import (
	"sync"
)

// StreamNotifier manages notifications for stream updates
type StreamNotifier struct {
	listeners map[string][]chan struct{}
	mutex     sync.RWMutex
}

// NewStreamNotifier creates a new stream notifier
func NewStreamNotifier() *StreamNotifier {
	return &StreamNotifier{
		listeners: make(map[string][]chan struct{}),
	}
}

// Subscribe creates a channel that will be notified when a stream is updated
func (sn *StreamNotifier) Subscribe(streamKey string) chan struct{} {
	sn.mutex.Lock()
	defer sn.mutex.Unlock()

	ch := make(chan struct{}, 1)
	sn.listeners[streamKey] = append(sn.listeners[streamKey], ch)
	return ch
}

// Unsubscribe removes a channel from the listener list
func (sn *StreamNotifier) Unsubscribe(streamKey string, ch chan struct{}) {
	sn.mutex.Lock()
	defer sn.mutex.Unlock()

	listeners := sn.listeners[streamKey]
	for i, listener := range listeners {
		if listener == ch {
			sn.listeners[streamKey] = append(listeners[:i], listeners[i+1:]...)
			close(ch)
			break
		}
	}

	if len(sn.listeners[streamKey]) == 0 {
		delete(sn.listeners, streamKey)
	}
}

// Notify notifies all listeners waiting on a stream
func (sn *StreamNotifier) Notify(streamKey string) {
	sn.mutex.RLock()
	listeners := sn.listeners[streamKey]
	sn.mutex.RUnlock()

	for _, ch := range listeners {
		select {
		case ch <- struct{}{}:
		default:
			// Channel already has a notification
		}
	}
}
