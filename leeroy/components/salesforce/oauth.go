package salesforce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	loginEndpoint    = "/services/oauth2/token"
	userInfoEndpoint = "/services/oauth2/userinfo"
)

type LoginConfig struct {
	ClientId     string
	ClientSecret string
	Username     string
	Password     string
	Endpoint     string
}

type LoginResponse struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
	ID          string `json:"id"`
	TokenType   string `json:"token_type"`
	IssuedAt    string `json:"issued_at"`
	Signature   string `json:"signature"`
}

type UserInfoResponse struct {
	UserID         string `json:"user_id"`
	OrganizationID string `json:"organization_id"`
}

func Login(cfg LoginConfig, timeout time.Duration) (*LoginResponse, error) {
	body := url.Values{}
	body.Set("grant_type", "password")
	body.Set("client_id", cfg.ClientId)
	body.Set("client_secret", cfg.ClientSecret)
	body.Set("username", cfg.Username)
	body.Set("password", cfg.Password)

	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Endpoint+loginEndpoint, strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code returned on OAuth authentication call: %v", httpResp.StatusCode)
	}

	var loginResponse LoginResponse
	err = json.NewDecoder(httpResp.Body).Decode(&loginResponse)
	if err != nil {
		return nil, err
	}

	return &loginResponse, nil
}

func UserInfo(endpoint string, accessToken string, timeout time.Duration) (*UserInfoResponse, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+userInfoEndpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code returned on OAuth user info call: %v", httpResp.StatusCode)
	}

	var userInfoResponse UserInfoResponse
	err = json.NewDecoder(httpResp.Body).Decode(&userInfoResponse)
	if err != nil {
		return nil, err
	}

	return &userInfoResponse, nil
}
