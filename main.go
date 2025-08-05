package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-ble/ble"
	"github.com/go-ble/ble/darwin"
)

const (
	SERVICE_UUID   = "0ffe3b09-5c3c-4afc-9832-8971e275d55e"
	URL_CHAR_UUID  = "a1b3981f-4057-436d-a68a-c9f8bd83158f"
	MIME_CHAR_UUID = "fd8508fa-61a2-482b-aa30-c328b9411371"
)

var debugMode bool

// debugf prints debug messages only wh				debugf("📦 Small chunk received (%d bytes) - assuming complete\n", len(data))n debug mode is enabled
func debugf(format string, args ...interface{}) {
	if debugMode {
		fmt.Printf("[DEBUG] "+format, args...)
	}
}

func main() {
	// Parse command line flags
	flag.BoolVar(&debugMode, "debug", false, "Enable debug output")
	flag.Parse()

	d, err := darwin.NewDevice()
	if err != nil {
		log.Fatalf("can't new device: %s", err)
	}
	ble.SetDefaultDevice(d)

	// Implement retry logic to handle connection failures
	maxRetries := 5
	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			fmt.Printf("\n🔄 Retrying connection attempt %d/%d...\n", retry+1, maxRetries)
			time.Sleep(3 * time.Second) // Extended retry interval
		}

		err := attemptConnection()
		if err == nil {
			fmt.Println("✅ Connection and data transfer completed successfully!")
			return
		}

		fmt.Printf("❌ Attempt %d failed: %s\n", retry+1, err)
		if retry == maxRetries-1 {
			log.Fatalf("All %d connection attempts failed", maxRetries)
		}
	}
}

func attemptConnection() error {
	// Create context with timeout
	mainCtx, mainCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer mainCancel()

	ctx := ble.WithSigHandler(mainCtx, func() {
		fmt.Println("\n🛑 Received interrupt signal, canceling...")
		mainCancel()
	})

	fmt.Println("🔍 Scanning for piping-now advertising devices...")
	fmt.Printf("Looking for service UUID: %s\n", SERVICE_UUID)

	// Step 1: Execute scan only (connection later)
	debugf("📡 Starting device scan (no connection yet)...\n")

	var targetDevice ble.Advertisement
	var deviceFound bool
	scanStartTime := time.Now()

	// Focus on scanning only (connection executed separately)
	scanCtx, scanCancel := context.WithTimeout(ctx, 20*time.Second)
	defer scanCancel()

	err := ble.Scan(scanCtx, false, func(a ble.Advertisement) {
		debugf("=== Found device ===\n")
		debugf("Device Name: '%s'\n", a.LocalName())
		debugf("Address: %s\n", a.Addr())
		debugf("RSSI: %d\n", a.RSSI())
		debugf("Connectable: %t\n", a.Connectable())
		debugf("Services: %v\n", a.Services())
		debugf("Scan Duration: %.1fs\n", time.Since(scanStartTime).Seconds())
		debugf("===================\n")

		// Skip devices that are not connectable
		if !a.Connectable() {
			debugf("⏭️ Device is not connectable, skipping...\n")
			return
		}

		// Search by service UUID (highest priority)
		for _, uuid := range a.Services() {
			uuidStr := uuid.String()
			normalizedFound := strings.ReplaceAll(strings.ToLower(uuidStr), "-", "")
			normalizedTarget := strings.ReplaceAll(strings.ToLower(SERVICE_UUID), "-", "")

			debugf("🔍 Comparing service UUID: %s\n", uuidStr)
			debugf("   Found (normalized): %s\n", normalizedFound)
			debugf("   Target (normalized): %s\n", normalizedTarget)

			if normalizedFound == normalizedTarget || strings.EqualFold(uuidStr, SERVICE_UUID) {
				fmt.Printf("✅ Found target device with matching service UUID!\n")
				fmt.Printf("   Service UUID: %s\n", uuidStr)
				fmt.Printf("   Device Name: %s\n", a.LocalName())
				fmt.Printf("   Address: %s\n", a.Addr())
				targetDevice = a
				deviceFound = true
				scanCancel() // Stop scan when device is found
				return
			}
		}

		// Fallback: Search by device name (lower priority)
		if a.LocalName() == "PipingSender" {
			fmt.Printf("📱 Found device with name 'PipingSender' but no matching UUID. Checking further...\n")
			// Don't immediately accept, continue scanning for UUID match
		}

		// Try partial match as fallback
		if strings.Contains(strings.ToLower(a.LocalName()), "piping") {
			fmt.Printf("📱 Found device with partial name match: %s (but preferring UUID match)\n", a.LocalName())
			// Don't immediately accept, continue scanning for UUID match
		}
	}, nil)

	if err != nil && err != context.Canceled {
		return fmt.Errorf("scan failed: %w", err)
	}

	if !deviceFound {
		return fmt.Errorf("target device with service UUID %s not found during scan", SERVICE_UUID)
	}

	// Step 2: Connect to the found device
	fmt.Printf("\n🔄 Target device found! Attempting connection...\n")
	fmt.Printf("Device: %s (%s)\n", targetDevice.LocalName(), targetDevice.Addr())

	// Set short timeout for connection
	connectCtx, connectCancel := context.WithTimeout(ctx, 8*time.Second)
	defer connectCancel()

	client, err := ble.Dial(connectCtx, targetDevice.Addr())
	if err != nil {
		return fmt.Errorf("connection to %s failed: %w", targetDevice.Addr(), err)
	}
	defer client.CancelConnection()

	fmt.Printf("✅ Connection established successfully!\n")
	debugf("🔗 Connected to device, checking connection state...\n")

	// Wait for connection stabilization
	debugf("⏳ Waiting for connection to stabilize (1s)...\n")
	time.Sleep(1 * time.Second)

	debugf("🔍 Starting service discovery...\n")

	// Set timeout for service discovery
	discoveryCtx, discoveryCancel := context.WithTimeout(ctx, 15*time.Second)
	defer discoveryCancel()

	// Use channels for asynchronous service discovery
	profileChan := make(chan *ble.Profile, 1)
	errorChan := make(chan error, 1)

	go func() {
		debugf("🔍 Calling DiscoverProfile(true)...\n")
		profile, err := client.DiscoverProfile(true)
		if err != nil {
			debugf("❌ DiscoverProfile failed: %s\n", err)
			errorChan <- err
		} else {
			debugf("✅ DiscoverProfile completed successfully\n")
			profileChan <- profile
		}
	}()

	var profile *ble.Profile
	select {
	case profile = <-profileChan:
		debugf("✅ Service discovery completed!\n")
	case err := <-errorChan:
		return fmt.Errorf("service discovery failed: %w", err)
	case <-discoveryCtx.Done():
		return fmt.Errorf("service discovery timeout: %w", discoveryCtx.Err())
	}

	debugf("Found %d services:\n", len(profile.Services))
	for _, s := range profile.Services {
		debugf("  Service: %s\n", s.UUID.String())
		for _, c := range s.Characteristics {
			debugf("    Characteristic: %s (Properties: %v)\n", c.UUID.String(), c.Property)
		}
	}

	// Wait a bit before GATT communication (connection stabilization)
	debugf("⏳ Waiting for GATT services to be ready...\n")
	time.Sleep(1 * time.Second)

	// Set timeout for GATT communication (extended for long data reads)
	gattCtx, gattCancel := context.WithTimeout(ctx, 45*time.Second)
	defer gattCancel()

	url, mimeType, err := readGattData(gattCtx, client, profile)
	if err != nil {
		return fmt.Errorf("GATT data reading failed: %w", err)
	}

	if url != "" && mimeType != "" {
		fmt.Printf("✅ Both URL and MIME type received successfully!\n")

		err := downloadFromPipingServer(url, mimeType)
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
		return nil
	} else {
		return fmt.Errorf("missing data - URL: '%s', MIME Type: '%s'", url, mimeType)
	}
}

func readGattData(ctx context.Context, client ble.Client, profile *ble.Profile) (string, string, error) {
	var url, mimeType string

	// Timeout channel
	done := make(chan struct{})
	var readErr error

	go func() {
		defer close(done)

		// Explore services and characteristics
		debugf("🔍 Starting GATT data reading from %d services...\n", len(profile.Services))

		for _, s := range profile.Services {
			debugf("🔍 Checking service: %s against %s\n", s.UUID.String(), SERVICE_UUID)

			// Add hyphen-removed comparison
			normalizedServiceUUID := strings.ReplaceAll(strings.ToLower(s.UUID.String()), "-", "")
			normalizedTargetUUID := strings.ReplaceAll(strings.ToLower(SERVICE_UUID), "-", "")

			if strings.EqualFold(s.UUID.String(), SERVICE_UUID) || normalizedServiceUUID == normalizedTargetUUID {
				debugf("✅ Found matching service! Processing %d characteristics...\n", len(s.Characteristics))

				for _, c := range s.Characteristics {
					debugf("🔍 Processing characteristic: %s\n", c.UUID.String())
					debugf("   Properties: %v\n", c.Property)

					// Set timeout for each read operation (for long data)
					readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)

					switch {
					case strings.EqualFold(c.UUID.String(), URL_CHAR_UUID):
						fmt.Printf("📱 Reading URL characteristic (UUID: %s, timeout: 20s)...\n", c.UUID.String())
						urlBytes, err := readCharacteristicWithTimeout(readCtx, client, c)
						readCancel()
						if err != nil {
							fmt.Printf("⚠️ Error reading URL characteristic: %s\n", err)
							continue
						}
						url = string(urlBytes)
						fmt.Printf("✅ Successfully received URL (%d bytes): %s\n", len(url), url)

					case strings.EqualFold(c.UUID.String(), MIME_CHAR_UUID):
						fmt.Printf("📱 Reading MIME characteristic (UUID: %s, timeout: 20s)...\n", c.UUID.String())
						mimeBytes, err := readCharacteristicWithTimeout(readCtx, client, c)
						readCancel()
						if err != nil {
							fmt.Printf("⚠️ Error reading MIME characteristic: %s\n", err)
							continue
						}
						mimeType = string(mimeBytes)
						fmt.Printf("✅ Successfully received MIME Type (%d bytes): %s\n", len(mimeType), mimeType)
					default:
						// Also try comparison with hyphens removed
						normalizedCharUUID := strings.ReplaceAll(strings.ToLower(c.UUID.String()), "-", "")
						normalizedUrlUUID := strings.ReplaceAll(strings.ToLower(URL_CHAR_UUID), "-", "")
						normalizedMimeUUID := strings.ReplaceAll(strings.ToLower(MIME_CHAR_UUID), "-", "")

						if normalizedCharUUID == normalizedUrlUUID {
							fmt.Printf("� Reading URL characteristic (normalized UUID: %s, timeout: 20s)...\n", c.UUID.String())
							urlBytes, err := readCharacteristicWithTimeout(readCtx, client, c)
							readCancel()
							if err != nil {
								fmt.Printf("⚠️ Error reading URL characteristic: %s\n", err)
								continue
							}
							url = string(urlBytes)
							fmt.Printf("✅ Successfully received URL (%d bytes): %s\n", len(url), url)
						} else if normalizedCharUUID == normalizedMimeUUID {
							fmt.Printf("📱 Reading MIME characteristic (normalized UUID: %s, timeout: 20s)...\n", c.UUID.String())
							mimeBytes, err := readCharacteristicWithTimeout(readCtx, client, c)
							readCancel()
							if err != nil {
								fmt.Printf("⚠️ Error reading MIME characteristic: %s\n", err)
								continue
							}
							mimeType = string(mimeBytes)
							fmt.Printf("✅ Successfully received MIME Type (%d bytes): %s\n", len(mimeType), mimeType)
						} else {
							readCancel()
							fmt.Printf("�🔍 Skipping unknown characteristic: %s (normalized: %s)\n", c.UUID.String(), normalizedCharUUID)
						}
					}
				}
			} else {
				debugf("⏭️ Skipping service: %s (not matching)\n", s.UUID.String())
			}
		}

		fmt.Printf("\n📊 Final results:\n")
		fmt.Printf("   URL: %s (length: %d)\n", url, len(url))
		fmt.Printf("   MIME: %s (length: %d)\n", mimeType, len(mimeType))

		if url == "" {
			readErr = fmt.Errorf("URL characteristic not found or empty")
			return
		}
		if mimeType == "" {
			readErr = fmt.Errorf("MIME type characteristic not found or empty")
			return
		}

		fmt.Printf("✅ All data successfully received!\n")
	}()

	select {
	case <-done:
		return url, mimeType, readErr
	case <-ctx.Done():
		return "", "", fmt.Errorf("GATT read timeout: %w", ctx.Err())
	}
}

// 特性読み取りをタイムアウト付きで実行
func readCharacteristicWithTimeout(ctx context.Context, client ble.Client, char *ble.Characteristic) ([]byte, error) {
	resultChan := make(chan struct {
		data []byte
		err  error
	}, 1)

	go func() {
		// BLE characteristic values can usually be read in one go, but may be split for long values
		// Try multiple reads to get complete data
		var allData []byte
		offset := 0
		maxRetries := 20 // Maximum 20 read attempts (for longer URLs)

		for i := 0; i < maxRetries; i++ {
			debugf("Read attempt %d/%d for characteristic %s...\n", i+1, maxRetries, char.UUID.String())

			data, err := client.ReadCharacteristic(char)
			if err != nil {
				debugf("Read error on attempt %d: %s\n", i+1, err)
				select {
				case resultChan <- struct {
					data []byte
					err  error
				}{allData, err}: // Return data read so far even if there's an error
				case <-ctx.Done():
				}
				return
			}

			debugf("Read %d bytes on attempt %d\n", len(data), i+1)

			if len(data) == 0 {
				// Reading complete when data is empty
				debugf("Read complete: total %d bytes in %d chunks\n", len(allData), i)
				break
			}

			// Debug display of data content
			if len(data) > 0 {
				preview := string(data)
				if len(preview) > 100 {
					preview = preview[:100] + "..."
				}
				debugf("Data content: %q\n", preview)
			}

			allData = append(allData, data...)
			offset += len(data)

			debugf("📦 Read chunk %d: %d bytes (offset: %d, total: %d)\n", i+1, len(data), offset, len(allData))

			// Reading complete if received data is less than max MTU size (usually 247-512 bytes)
			// Data from Android may be split by MTU size
			if len(data) < 200 {
				fmt.Printf("� Small chunk received (%d bytes) - assuming complete\n", len(data))
				break
			}

			// Add small wait to wait for next chunk preparation
			time.Sleep(50 * time.Millisecond)
		}

		if len(allData) == 0 {
			select {
			case resultChan <- struct {
				data []byte
				err  error
			}{nil, fmt.Errorf("no data received")}:
			case <-ctx.Done():
			}
			return
		}

		select {
		case resultChan <- struct {
			data []byte
			err  error
		}{allData, nil}:
		case <-ctx.Done():
		}
	}()

	select {
	case result := <-resultChan:
		return result.data, result.err
	case <-ctx.Done():
		return nil, fmt.Errorf("characteristic read timeout: %w", ctx.Err())
	}
}

func downloadFromPipingServer(url, mimeType string) error {
	fmt.Printf("Downloading data from: %s\n", url)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return handleReceivedData(data, mimeType)
}

func handleReceivedData(data []byte, mimeType string) error {
	fmt.Printf("Received %d bytes with MIME type: %s\n", len(data), mimeType)

	switch {
	case strings.HasPrefix(mimeType, "text/"):
		// テキストデータの場合は表示
		fmt.Printf("Text content:\n%s\n", string(data))

	case strings.HasPrefix(mimeType, "image/"):
		// 画像ファイルの場合は保存
		filename := fmt.Sprintf("received_image_%d.%s",
			time.Now().Unix(), getExtensionFromMime(mimeType))
		return saveToFile(data, filename)

	default:
		// その他のファイルは汎用的に保存
		filename := fmt.Sprintf("received_file_%d.bin", time.Now().Unix())
		return saveToFile(data, filename)
	}

	return nil
}

func saveToFile(data []byte, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	fmt.Printf("Data saved to: %s\n", filename)
	return nil
}

func getExtensionFromMime(mimeType string) string {
	switch mimeType {
	case "image/jpeg":
		return "jpg"
	case "image/png":
		return "png"
	case "text/plain":
		return "txt"
	default:
		return "bin"
	}
}
