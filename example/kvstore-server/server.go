package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"io"
	"log"
	"log/slog"
	"net/http"
	"sort"
	"time"
)

type HTTPServer struct {
	addr     string
	mntPoint string
	store    *KVStore
	logger   *slog.Logger
}

func NewHTTPServer(addr, mntPoint string, store *KVStore, logger *slog.Logger) *HTTPServer {
	return &HTTPServer{
		addr:     addr,
		mntPoint: mntPoint,
		store:    store,
		logger:   logger,
	}
}

func (s *HTTPServer) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.addr,
		Handler: s.Routes(),
	}

	shutdownCh := make(chan struct{})
	go func(ctx context.Context) {
		<-ctx.Done()
		s.logger.Info("Shutting down kvstore http server")
		tCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(tCtx); err != nil {
			s.logger.Error("Error shutting down kvstore http server, force closing",
				"err", err.Error())
			_ = srv.Close()
		}
		close(shutdownCh)
	}(ctx)
	s.logger.Info("Starting kvstore http server")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownCh
	return nil
}

func (s *HTTPServer) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route(s.mntPoint, func(r chi.Router) {
		r.Get("/state", s.GetState)
		r.Get("/vals", s.Values)
		r.Get("/{key}", s.Get)
		r.Post("/", s.Post)
		r.Put("/{key}", s.Put)
	})
	return r
}

func (s *HTTPServer) Get(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	v, ok, err := s.store.Get(key)
	if err != nil && errors.Is(err, ErrStoreNotReady) {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(v))
}

func (s *HTTPServer) Put(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	_, ok, err := s.store.Get(key)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	err = s.store.Put(key, string(value))
	if err != nil && errors.Is(err, ErrStoreNotReady) {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	}
}

func (s *HTTPServer) Post(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	var e map[string]string
	err = json.Unmarshal(data, &e)
	if err != nil {
		s.logger.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	key, value := e["key"], e["value"]
	s.logger.Info("POST request", slog.String("key", key), slog.String("value", value))
	v, ok, err := s.store.Get(key)
	if ok {
		s.logger.Error("err: key already exists", "key", key, "value", v)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	err = s.store.Put(key, value)
	if err != nil {
		if errors.Is(err, ErrStoreNotReady) {
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		s.logger.Error(err.Error(), "post-key", key)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte("OK"))
}

func (s *HTTPServer) GetState(w http.ResponseWriter, _ *http.Request) {
	c := s.store.Count()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("%d", c)))
}

func (s *HTTPServer) Values(w http.ResponseWriter, _ *http.Request) {
	vals := make([]string, 0)
	for v := range s.store.Values() {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(vals)
	_, _ = w.Write(b)
}
