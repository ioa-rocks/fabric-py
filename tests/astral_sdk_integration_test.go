// This test is overlaid into Fabric's astral/daemon tests by
// run_daemon_tests.py, reusing its real two-node fixture without modifying
// the Fabric checkout. Only disposable state and loopback are used.
package daemon_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"ioa.rocks/fabric/astral"
	"ioa.rocks/fabric/astral/daemon"
	"ioa.rocks/fabric/cert"
	"ioa.rocks/fabric/circle"
	fabricdaemon "ioa.rocks/fabric/daemon"
)

func TestPythonSDKCredentials(t *testing.T) {
	sdk := os.Getenv("FABRIC_PY_SDK")
	if sdk == "" {
		t.Fatal("FABRIC_PY_SDK must name the SDK checkout")
	}
	_, root, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nas := newMeshNode(t, root, "nas")
	desk := newMeshNode(t, root, "desk")
	roster := []*cert.Cert{nas.cert, desk.cert}
	x, err := circle.Init(nas.circleRoot(), "sdk", "pw")
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := x.Export()
	if err != nil {
		t.Fatal(err)
	}
	admitInto(t, nas, x)
	joinCircle(t, desk, bundle, true)
	admits(t, nas, x, desk)
	fnas := nas.fabric(t, root, "nas", nil, roster...)
	startDaemon(t, nas.statePath, fnas)
	fdesk := desk.fabric(t, root, "desk", nil, roster...)
	startDaemon(t, desk.statePath, fdesk)
	fdesk.Host.Peerstore().AddAddrs(fnas.Host.ID(), fnas.Host.Addrs(), time.Hour)
	if err := fdesk.Host.Connect(t.Context(), fdesk.Host.Peerstore().PeerInfo(fnas.Host.ID())); err != nil {
		t.Fatal(err)
	}
	clDesk := daemon.NewClient(desk.statePath).In(x.Config.DID)
	clNas := daemon.NewClient(nas.statePath).In(x.Config.DID)
	waitConnected(t, clDesk, "nas")
	for name, client := range map[string]*daemon.Client{
		"subject:queues/local": clDesk, "subject:queues/remote": clNas,
		"subject:queues/other": clNas,
	} {
		if err := client.Declare(name, astral.DeliveryShared, nil); err != nil {
			t.Fatal(err)
		}
	}
	waitPlaced(t, clDesk, "subject:queues/remote @ "+x.Config.DID)

	keys := map[string]string{}
	records := map[string]*fabricdaemon.IssuedKey{}
	for label, verbs := range map[string][]string{
		"publisher": {"publish"}, "worker": {"claim"},
		"other": {"publish"}, "expired": {"publish"},
	} {
		validity := time.Hour
		if label == "expired" {
			validity = time.Second
		}
		key, err := fdesk.IssueKey(x.Config.DID,
			[]string{"subject:queues/local", "subject:queues/remote"}, verbs, validity, label)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), label+".key")
		if err := astral.WriteKeyFile(path, astral.KeyFile{
			ID: key.ID, DID: key.DID, Circle: key.Circle, Res: key.Res,
			Verbs: key.Verbs, Exp: key.Exp,
			Seed: base64.StdEncoding.EncodeToString(key.Seed), Chain: base64.StdEncoding.EncodeToString(key.Chain),
		}); err != nil {
			t.Fatal(err)
		}
		keys[label], records[label] = path, key
	}
	manifest := filepath.Join(t.TempDir(), "fixture.json")
	data, err := json.Marshal(map[string]any{
		"state": desk.statePath, "circle": x.Config.DID, "keys": keys,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(stage string) {
		t.Helper()
		cmd := exec.Command("python3", filepath.Join(sdk, "tests", "daemon_scenario.py"), manifest, stage)
		cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(sdk, "src"))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Python %s: %v\n%s", stage, err, out)
		}
		t.Log(strings.TrimSpace(string(out)))
	}
	run("allowed")
	if err := fdesk.RevokeKey(records["worker"].ID); err != nil {
		t.Fatal(err)
	}
	// Require the real remote verifier to have learned the revocation.
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := clDesk.WithKey(signerFor(t, records["worker"])).Claim("subject:queues/remote", "revocation-check", 30, 1)
		if err != nil && strings.Contains(err.Error(), "denied") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("remote host did not learn sub-key revocation")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(time.Until(time.Unix(records["expired"].Exp+1, 0)))
	run("revoked")
}
