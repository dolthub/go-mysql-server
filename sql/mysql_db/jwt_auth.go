// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql_db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	jose "gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

var ErrKeyNotFound = errors.New("Key not found")

// TODO: log jwt fields from jwks config
func validateJWT(config []JwksConfig, username, identity, token string) (bool, error) {
	// Verify that the JWKS for the configured authentication exists.
	if len(config) == 0 {
		return false, nil
	}
	parsed, err := jwt.ParseSigned(token)
	if err != nil {
		return false, err
	}
	if len(parsed.Headers) != 1 {
		return false, fmt.Errorf("ValidateJWT: Unexpected JWT headers length %v.", len(parsed.Headers))
	}
	parsedIdentity := parseUserIdentity(identity)
	jwksConfig, err := getMatchingJwksConfig(config, parsedIdentity["name"])
	if err != nil {
		return false, err
	}
	if parsedIdentity["sub"] != username {
		return false, fmt.Errorf("ValidateJWT: Subjects do not match")
	}
	// Verify that the alg in the incoming JWT matches the alg config in the JWKS.
	if jwksConfig.Claims["alg"] != parsed.Headers[0].Algorithm {
		return false, fmt.Errorf("ValidateJWT: Algorithms do not match")
	}
	// Verify the signature of the JWT using the JWKS contents fetched from the configured location.
	fetchedJwks, err := getJWKSFromUrl(jwksConfig.LocationUrl)
	if err != nil {
		return false, err
	}

	keyID := parsed.Headers[0].KeyID
	keys := fetchedJwks.Key(keyID)

	var claims jwt.Claims
	claimsError := fmt.Errorf("ValidateJWT: KeyID: %v. Err: %w", keyID, ErrKeyNotFound)
	for _, key := range keys {
		claimsError = parsed.Claims(key.Key, &claims)
		if claimsError == nil {
			break
		}
	}
	if claimsError != nil {
		return false, claimsError
	}

	// Verify that the signed JWT is not expired and does not violate "nbf"
	// Verify all claims on the JWT as configured in the USER and the JWKS.
	expectedClaims := getExpectedClaims(jwksConfig.Claims)
	if err := claims.Validate(expectedClaims.WithTime(time.Now())); err != nil {
		return false, err
	}
	return true, nil
}

func getExpectedClaims(claims map[string]string) jwt.Expected {
	// expectedClaims := make(jwt.Expected{}, len(claims))
	var expectedClaims jwt.Expected
	for cl, value := range claims {
		switch cl {
		case "iss":
			expectedClaims.Issuer = value
		case "sub":
			expectedClaims.Subject = value
		case "aud":
			expectedClaims.Audience = []string{value}
		}
	}
	return expectedClaims
}

func getMatchingJwksConfig(config []JwksConfig, name string) (*JwksConfig, error) {
	for _, item := range config {
		if item.Name == name {
			return &item, nil
		}
	}
	return nil, fmt.Errorf("ValidateJWT: Matching JWKS config not found")
}

func getJWKSFromUrl(locationUrl string) (*jose.JSONWebKeySet, error) {
	client := &http.Client{}

	request, err := http.NewRequest("GET", locationUrl, nil)
	if err != nil {
		return nil, err
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	} else if response.StatusCode/100 != 2 {
		return nil, errors.New("FetchedJWKS: Non-2xx status code from JWKS fetch")
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}

		jwks := jose.JSONWebKeySet{}
		err = json.Unmarshal(contents, &jwks)
		if err != nil {
			return nil, err
		}
		return &jwks, nil
	}
}

func parseUserIdentity(identity string) map[string]string {
	idMap := make(map[string]string)
	items := strings.Split(identity, ",")
	for _, item := range items {
		tup := strings.Split(item, "=")
		idMap[tup[0]] = tup[1]
	}
	return idMap
}
