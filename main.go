// ssc3_serial_proxy project main.go
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maledog/logrot"
	"github.com/maledog/tcp_server"

	"github.com/kardianos/osext"
)

import "github.com/tarm/serial"

type F_Config struct {
	Log   string   `json:"log"`
	Ports []F_Port `json:"ports"`
}

type F_Port struct {
	Debug               bool             `json:"debug"`
	Is_Enabled          bool             `json:"enabled"`
	Path                string           `json:"path"`
	Baud                int              `json:"baud_rate"`
	DataBits            int              `json:"data_bits"`
	Parity              string           `json:"parity"`
	StopBits            int              `json:"stop_bits"`
	Response_timeout_ms int              `json:"response_timeout_ms"`
	Num                 int              `json:"number_of_polling"`
	Tcp_listeners       []F_Tcp_listener `json:"tcp_listeners"`
}

type F_Tcp_listener struct {
	Is_enabled        bool   `json:"enabled"`
	Tcp_listen        string `json:"tcp_listen"`
	Tcp_io_timeout_ms int    `json:"tcp_io_timeout_ms"`
	Protocol          string `json:"protocol"` //"raw, mbtcp2mbrtu, mbtcp2ivtm"
}

type Config struct {
	Log   string
	Ports []Port
}

type Port struct {
	Debug               bool
	Is_Enabled          bool
	Path                string
	Baud                int
	DataBits            int
	Parity              string
	StopBits            int
	Response_timeout_ms time.Duration
	Num                 int
	Tcp_listeners       []Tcp_listener
}

type Tcp_listener struct {
	Is_enabled        bool
	Tcp_listen        string
	Tcp_io_timeout_ms time.Duration
	Protocol          int //"raw - 1, mbtcp2mbrtu - 2, mbtcp2ivtm - 3"
}

func main() {
	exe_dir, err := osext.ExecutableFolder()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	var in_config string
	var make_config bool
	var ds bool
	flag.StringVar(&in_config, "c", exe_dir+"/config.json", "config file")
	flag.BoolVar(&make_config, "m", false, "make config file")
	flag.BoolVar(&ds, "ds", false, "debug serial stdout")
	flag.Parse()
	if make_config {
		cf, err := make_f_config(in_config)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Config file:%s\n", cf)
			os.Exit(0)
		}
	}
	f_cnf, err := get_f_config(in_config)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	cnf, err := get_config(f_cnf)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if ds {
		for i := 0; i < len(cnf.Ports); i++ {
			cnf.Ports[i].Debug = true
		}
		cnf.Log = ""
	}
	if cnf.Log != "" {
		f, err := logrot.Open(cnf.Log, 0644, 10*1024*1024, 10)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	ch_os_signals := make(chan os.Signal, 1)
	signal.Notify(ch_os_signals, os.Interrupt, os.Kill, syscall.SIGTERM)
	chs_serial_close := make(map[string]chan struct{})
	n := 0
	for _, port := range cnf.Ports {
		serial_conn, err := open_port(port)
		if err != nil {
			log.Printf("SERIAL: ERROR:%v.\n", err)
			continue
		}
		defer serial_conn.Close()
		log.Printf("Opened port %q\n", port.Path)
		ch_start_error := make(chan error, 1)
		ch_serial := make(chan Message, 100)
		for _, listener := range port.Tcp_listeners {
			go start_server(listener, ch_serial, ch_start_error, port.Debug)
			time.Sleep(2 * time.Second)
			select {
			case err = <-ch_start_error:
				log.Println(err)
				continue
			default:
			}
			log.Printf("Listen %q, proto: %d for serial port %q\n", listener.Tcp_listen, listener.Protocol, port.Path)
		}
		ch_serial_close := make(chan struct{}, 1)
		chs_serial_close[port.Path] = ch_serial_close
		go serial_worker(serial_conn, port, ch_serial, ch_serial_close)
		n++
	}
	if n > 0 {
		log.Println("Program launched.")
		<-ch_os_signals
		log.Println("Interrupted program.")
		for _, port := range cnf.Ports {
			chs_serial_close[port.Path] <- struct{}{}
		}
	} else {
		log.Println("Open 0 serial ports. The program is closed.")
	}
}

func open_port(port Port) (*serial.Port, error) {
	c := &serial.Config{
		Name:        port.Path,
		Baud:        port.Baud,
		ReadTimeout: port.Response_timeout_ms,
	}
	switch port.Parity {
	case "O":
		{
			c.Parity = serial.ParityOdd
		}
	case "E":
		{
			c.Parity = serial.ParityEven
		}
	default:
		{
			c.Parity = serial.ParityNone
		}

	}
	c.Size = uint8(port.DataBits)
	switch port.StopBits {
	case 2:
		{
			c.StopBits = serial.Stop2
		}
	default:
		{
			c.StopBits = serial.Stop1
		}
	}
	return serial.OpenPort(c)
}

func serial_worker(serial_conn *serial.Port, port Port, ch_serial chan Message, ch_serial_close chan struct{}) {
	defer serial_conn.Close()
	reader := bufio.NewReader(serial_conn)
	writer := bufio.NewWriter(serial_conn)
	for {
		select {
		case <-ch_serial_close:
			{
				break
			}
		default:
			{
				var req []byte
				var resp []byte
				var err error
				m := <-ch_serial
				if port.Debug {
					log.Printf("TCP: %v << %v: % x\n", m.Client.Conn().LocalAddr(), m.Client.Conn().RemoteAddr(), m.Data)
				}
				switch m.Type {
				case 1:
					{
						req = m.Data
						if port.Debug {
							log.Printf("SERIAL: % x >> %q\n", req, port.Path)
						}
						for i := 0; i <= port.Num; i++ {
							resp, err = write_data(req, reader, writer)
							if err != nil {
								time.Sleep(200 * time.Millisecond)
								continue
							}
							break
						}
						if err != nil {
							if port.Debug {
								log.Printf("SERIAL:%v\n", err)
							}
							resp = []byte{0xff}
						}
						if port.Debug {
							log.Printf("SERIAL: % x << %q\n", resp, port.Path)
						}
					}
				case 2:
					{
						mtr, err := parse_mbtcp_request(m.Data)
						if err != nil {
							log.Println(err)
							resp = []byte{0xff}
						} else {
							req, err = mbtcp2mbrtu(m.Data)
							if err != nil {
								log.Println(err)
								resp = make_modbus_error(mtr.Addr, mtr.Func, 4)
							} else {
								if port.Debug {
									log.Printf("SERIAL: % x >> %q\n", req, port.Path)
								}
								for i := 0; i <= port.Num; i++ {
									resp, err = write_data(req, reader, writer)
									if err != nil {
										time.Sleep(200 * time.Millisecond)
										continue
									}
									break
								}
								if err != nil {
									if port.Debug {
										log.Printf("SERIAL:%v\n", err)
									}
									resp = make_modbus_error(mtr.Addr, mtr.Func, 4)
								} else {
									if port.Debug {
										log.Printf("SERIAL: % x << %q\n", resp, port.Path)
									}
									resp, err = mbrtu2mbtcp(resp)
									if err != nil {
										log.Println(err)
										resp = make_modbus_error(mtr.Addr, mtr.Func, 4)
									}
								}
							}
						}
					}
				case 3:
					{
						mtr, err := parse_mbtcp_request(m.Data)
						if err != nil {
							log.Println(err)
							resp = []byte{0xff}
						} else {
							resp = ivtm_check_req_error(mtr)
							if len(resp) == 0 {
								req = make_ivtm_request(mtr.Addr)
								if port.Debug {
									log.Printf("SERIAL: % x >> %q\n", req, port.Path)
								}
								for i := 0; i <= port.Num; i++ {
									resp, err = write_data(req, reader, writer)
									if err != nil {
										time.Sleep(200 * time.Millisecond)
										continue
									}
									break
								}
								if err != nil {
									if port.Debug {
										log.Printf("SERIAL:%v\n", err)
									}
									resp = make_modbus_error(mtr.Addr, mtr.Func, 4)
								} else {
									temp, humd, err := parse_ivtm_response(resp, mtr.Addr)
									if err != nil {
										log.Println(err)
										resp = make_modbus_error(mtr.Addr, mtr.Func, 4)
									} else {
										resp = make_mbtcp_response(mtr, temp, humd)
									}
								}
							}
						}
					}
				}
				if len(resp) == 0 {
					resp = []byte{0xff}
				}
				if port.Debug {
					log.Printf("TCP: %v >> %v: % x\n", m.Client.Conn().LocalAddr(), m.Client.Conn().RemoteAddr(), resp)
				}
				err = m.Client.Send(resp)
				if err != nil {
					log.Println(err)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}

func write_data(req []byte, reader *bufio.Reader, writer *bufio.Writer) (resp []byte, err error) {
	_, err = writer.Write(req)
	if err != nil {
		return
	}
	err = writer.Flush()
	if err != nil {
		return
	}
	buf := make([]byte, 256)
	time.Sleep(200 * time.Millisecond)
	n, err := reader.Read(buf)
	if err != nil {
		return
	}
	resp = buf[:n]
	return
}

type Message struct {
	Client *tcp_server.Client
	Type   int
	Data   []byte
}

func start_server(listener Tcp_listener, ch_serial chan Message, ch_start_error chan error, debug bool) {
	server := tcp_server.New("tcp4", listener.Tcp_listen)
	server.SetReadBufferSize(512)
	server.OnNewClient(func(c *tcp_server.Client) {
		if debug {
			log.Println("connected", c.Conn().RemoteAddr())
		}
	})
	server.OnNewMessage(func(c *tcp_server.Client, message []byte) {
		var m Message
		m.Client = c
		m.Type = listener.Protocol
		m.Data = message
		select {
		case ch_serial <- m:
		default:
		}
	})
	server.OnClientConnectionClosed(func(c *tcp_server.Client, err error) {
		if debug {
			log.Println("disconnected", c.Conn().RemoteAddr())
		}
	})
	err := server.Listen()
	if err != nil {
		ch_start_error <- err
	}
}

func get_config(f_cnf F_Config) (cnf Config, err error) {
	cnf.Log = f_cnf.Log
	for _, f_port := range f_cnf.Ports {
		if f_port.Is_Enabled {
			var port Port
			port.Is_Enabled = f_port.Is_Enabled
			port.Debug = f_port.Debug
			port.Path = f_port.Path
			port.Baud = f_port.Baud
			port.DataBits = f_port.DataBits
			port.Parity = f_port.Parity
			port.StopBits = f_port.StopBits
			port.Response_timeout_ms = time.Duration(f_port.Response_timeout_ms) * time.Millisecond
			port.Num = f_port.Num
			for _, fl := range f_port.Tcp_listeners {
				if fl.Is_enabled {
					var l Tcp_listener
					l.Is_enabled = fl.Is_enabled
					l.Tcp_listen = fl.Tcp_listen
					l.Tcp_io_timeout_ms = time.Duration(fl.Tcp_io_timeout_ms) * time.Millisecond
					switch fl.Protocol {
					case "raw":
						l.Protocol = 1
					case "mbtcp2mbrtu":
						l.Protocol = 2
					case "mbtcp2ivtm":
						l.Protocol = 3
					default:
						{
							log.Printf("unknown protocol type on listener %s\n", l.Tcp_listen)
							continue
						}
					}
					port.Tcp_listeners = append(port.Tcp_listeners, l)
				}
			}
			cnf.Ports = append(cnf.Ports, port)
		}
	}
	return
}

func get_f_config(cf string) (cnf F_Config, err error) {
	data, err := ioutil.ReadFile(cf)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cnf)
	return
}

func make_f_config(cf string) (string, error) {
	var cnf F_Config
	var port F_Port
	var listener F_Tcp_listener
	cnf.Log = "logs/ssc3_serial_proxy.log"
	port.Debug = false
	port.Is_Enabled = true
	port.Path = "/dev/ttyAPP1"
	port.Baud = 9600
	port.DataBits = 8
	port.Parity = "N"
	port.StopBits = 1
	port.Response_timeout_ms = 2000
	port.Num = 1
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15500"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "raw"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15501"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "mbtcp2mbrtu"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15502"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "mbtcp2ivtm"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	cnf.Ports = append(cnf.Ports, port)
	port.Debug = false
	port.Is_Enabled = true
	port.Path = "/dev/ttyAPP4"
	port.Baud = 9600
	port.DataBits = 8
	port.Parity = "N"
	port.StopBits = 1
	port.Response_timeout_ms = 2000
	port.Num = 1
	port.Tcp_listeners = []F_Tcp_listener{}
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15503"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "raw"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15504"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "mbtcp2mbrtu"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	listener.Is_enabled = true
	listener.Tcp_listen = "127.0.0.1:15505"
	listener.Tcp_io_timeout_ms = 5000
	listener.Protocol = "mbtcp2ivtm"
	port.Tcp_listeners = append(port.Tcp_listeners, listener)
	cnf.Ports = append(cnf.Ports, port)
	data, err := json.MarshalIndent(cnf, "", "\t")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(cf, data, 0644)
	if err != nil {
		return "", err
	}
	return cf, nil
}

func fso_exist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

type MbTcpReq struct {
	Addr     uint8
	Func     uint8
	StartReg uint16
	Val      uint16
}

func parse_mbtcp_request(adu []byte) (mtr MbTcpReq, err error) {
	if len(adu) > 260 || len(adu) < 6 {
		err = errors.New("ADU Modbus TCP length must be between 6 and 260 bytes.")
		return
	}
	apdu := adu[6:]
	len_apdu := binary.BigEndian.Uint16(adu[4:6])
	if int(len_apdu) != len(apdu) {
		err = errors.New("PDU field does not match length of the PDU.")
		return
	}
	mtr.Addr = uint8(adu[6])
	mtr.Func = uint8(adu[7])
	mtr.StartReg = binary.BigEndian.Uint16(adu[8:10])
	mtr.Val = binary.BigEndian.Uint16(adu[10:12])
	return
}

func ivtm_check_req_error(mtr MbTcpReq) (resp []byte) {
	if !(mtr.Func == 3 || mtr.Func == 4) {
		resp = make_modbus_error(mtr.Addr, mtr.Func, 1)
		return
	}

	if !(mtr.StartReg == 0 || mtr.StartReg == 2) {
		resp = make_modbus_error(mtr.Addr, mtr.Func, 2)
		return
	}

	if !(mtr.Val == 2 || mtr.Val == 4) {
		resp = make_modbus_error(mtr.Addr, mtr.Func, 3)
		return
	}

	if mtr.StartReg == 2 && mtr.Val != 2 {
		resp = make_modbus_error(mtr.Addr, mtr.Func, 3)
	}
	return
}

func make_modbus_error(addr uint8, mb_func uint8, e uint8) []byte {
	return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x03, byte(addr), byte(mb_func) ^ 0x80, byte(e)}
}

func make_mbtcp_response(mtr MbTcpReq, temp float32, humd float32) (data []byte) {
	header := []byte{0x00, 0x00, 0x00, 0x00, 0x00}
	body := []byte{byte(mtr.Addr), byte(mtr.Func)}
	switch {
	case mtr.StartReg == 0 && mtr.Val == 4:
		{
			body = append(body, 0x08)
			body = append(body, float32_to_byte(temp)...)
			body = append(body, float32_to_byte(humd)...)
			data = append(header, byte(len(body)))
			data = append(data, body...)
		}
	case mtr.StartReg == 0 && mtr.Val == 2:
		{
			body = append(body, 0x04)
			body = append(body, float32_to_byte(temp)...)
			data = append(header, byte(len(body)))
			data = append(data, body...)
		}
	case mtr.StartReg == 2 && mtr.Val == 2:
		{
			body = append(body, 0x04)
			body = append(body, float32_to_byte(humd)...)
			data = append(header, byte(len(body)))
			data = append(data, body...)
		}
	default:
		{
			data = make_modbus_error(mtr.Addr, mtr.Func, 4)
		}
	}
	return
}

func make_ivtm_request(addr uint8) []byte {
	ascii_packet := fmt.Sprintf("$%04XRR000008", addr)
	_, h, l := ivtm_get_check_sum([]byte(ascii_packet))
	ascii_packet = fmt.Sprintf("%s%X%X\r", ascii_packet, h, l)
	return []byte(ascii_packet)
}

func parse_ivtm_response(resp []byte, d_addr uint8) (temp float32, humd float32, err error) {
	n := len(resp)
	if n < 4 {
		err = errors.New("ivtm response length less than 4")
		return
	}
	if resp[0] != 0x041 && resp[n-1] != 0x00d {
		err = errors.New("wrong ivtm response")
		return
	}
	body := resp[:n-3]
	_, h, l := ivtm_get_check_sum(body)
	if string(resp[n-3:n-1]) != fmt.Sprintf("%X%X", h, l) {
		err = errors.New("ivtm checksum error")
		return
	}
	addr := string(resp[1:5])
	if addr != fmt.Sprintf("%04X", d_addr) {
		err = errors.New("ivtm how can this be? wrong address")
		return
	}
	temp, err = ivtm_runes_to_float32([]rune(string(resp[7:15])))
	if err != nil {
		return
	}
	humd, err = ivtm_runes_to_float32([]rune(string(resp[15:23])))
	if err != nil {
		return
	}
	return
}

func mbtcp2mbrtu(adu []byte) ([]byte, error) {
	if len(adu) > 260 || len(adu) < 6 {
		return adu, errors.New("ADU Modbus TCP length must be between 6 and 260 bytes.")
	}
	apdu := adu[6:]
	len_apdu := binary.BigEndian.Uint16(adu[4:6])
	if int(len_apdu) != len(apdu) {
		return apdu, errors.New("PDU field does not match length of the PDU.")
	}
	crc16 := modbus_crc16(apdu)
	return append(apdu, crc16...), nil
}

func mbrtu2mbtcp(adu []byte) ([]byte, error) {
	if len(adu) > 256 || len(adu) < 2 {
		return adu, errors.New("ADU Modbus RTU length must be between 2 and 256 bytes.")
	}
	pdu_crc := adu[len(adu)-2:]
	apdu := adu[:len(adu)-2]
	crc16 := modbus_crc16(apdu)
	if pdu_crc[0] != crc16[0] || pdu_crc[1] != crc16[1] {
		return adu, errors.New("Error CRC16 Modbus RTU.")
	}
	if binary.BigEndian.Uint16(pdu_crc) != binary.BigEndian.Uint16(crc16) {
		return adu, errors.New("Error CRC16 Modbus RTU.")
	}
	modbus_tcp_header := []byte{0x00, 0x00, 0x00, 0x00, byte(len(apdu) >> 8), byte(len(apdu) & 0xff)}
	return append(modbus_tcp_header, apdu...), nil
}

func modbus_crc16(data []byte) []byte {
	var crc16 uint16 = 0xffff
	l := len(data)
	for i := 0; i < l; i++ {
		crc16 ^= uint16(data[i])
		for j := 0; j < 8; j++ {
			if crc16&0x0001 > 0 {
				crc16 = (crc16 >> 1) ^ 0xA001
			} else {
				crc16 >>= 1
			}
		}
	}
	packet := make([]byte, 2)
	packet[0] = byte(crc16 & 0xff)
	packet[1] = byte(crc16 >> 8)
	return packet
}

func ivtm_get_check_sum(bytes_arr []byte) (m, h, l byte) {
	total := 0
	for _, value := range bytes_arr {
		total += int(value)
	}
	mod := (total % 0x0100)
	return byte(mod), byte((mod >> 4) & 0xf), byte(mod & 0xf)
}

func ivtm_runes_to_float32(runes []rune) (float32, error) {
	var fl32 float32
	bytes, err := hex.DecodeString(string(runes))
	if err != nil {
		return fl32, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(bytes)), nil
}

func float32_to_byte(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func byte_to_float32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func float64_to_byte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func byte_to_float64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}
