// Package lock is a small Redis lock for work that must not run twice at once.
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// CallbackTTL is how long a callback lock lives if its holder dies.
const CallbackTTL = 3000 * time.Millisecond

// CallbackKey is the lock key of one supplier order.
func CallbackKey(supplier, supplierOrderNo string) string {
	return "lock:cb:" + supplier + ":" + supplierOrderNo
}

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

type Redis struct {
	rdb redis.UniversalClient
}

func New(rdb redis.UniversalClient) *Redis { return &Redis{rdb: rdb} }

// Acquire takes the lock with a random token and a millisecond expiry.
// ok=false means someone else holds it.
func (l *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (token string, ok bool, err error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", false, fmt.Errorf("lock %s: token: %w", key, err)
	}
	token = hex.EncodeToString(buf)
	err = l.rdb.Do(ctx, "SET", key, token, "NX", "PX", ttl.Milliseconds()).Err()
	switch {
	case errors.Is(err, redis.Nil):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("lock %s: %w", key, err)
	}
	return token, true, nil
}

// Release deletes the lock only if it still carries token, so a holder whose
// lock expired cannot free the next holder's lock.
func (l *Redis) Release(ctx context.Context, key, token string) error {
	if err := releaseScript.Run(ctx, l.rdb, []string{key}, token).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("unlock %s: %w", key, err)
	}
	return nil
}
