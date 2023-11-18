package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type proposal struct {
	id    int
	value rune
}

var (
	hosts            []string
	PORT             = "4950"
	isProposer       bool
	isAcceptor       bool
	isLearner        bool
	minProposal      int
	acceptorMap      map[string]int
	proposerMap      map[string]int
	learnerMap       map[string]int
	acceptedProposal proposal
	acceptances      int
	myProposal       proposal
	processNumbers   []int
	hostname         string
)

func checkIfError(e error, message string) {
	if e != nil {
		fmt.Fprintln(os.Stderr, message)
		//panic(e)
	}
}

func currentHostRole(line string) {
	switch {
	case strings.Contains(line, "proposer"):
		isProposer = true

	case strings.Contains(line, "acceptor"):
		isAcceptor = true
	case strings.Contains(line, "learner"):
		isLearner = true
	}
}

// read hostsfile
// assign self as one of the types
func gethosts(filename string) {
	file, err := os.Open(filename)
	checkIfError(err, "Error opening file:")

	defer file.Close()

	hostname, err = os.Hostname()
	checkIfError(err, "Error getting hostname:")

	acceptorMap = make(map[string]int)
	proposerMap = make(map[string]int)
	learnerMap = make(map[string]int)

	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)

	// Iterate over each line in the file
	for scanner.Scan() {
		line := scanner.Text()

		// Check for errors during scanning
		err := scanner.Err()
		checkIfError(err, "Error reading file:")

		// appends each host to the hosts list by splicing the line
		hosts = append(hosts, line[0:5])

		// assigns the current peer its role
		if strings.Contains(line, hostname) {
			currentHostRole(line)
		}

		// splits each line to get the peer name and role separated
		parts := strings.Split(line, ":")
		peer_name := parts[0]

		role := parts[1]
		//fmt.Fprintln(os.Stdout, peer_name)
		//fmt.Fprintln(os.Stdout, role)

		// if the peer has multiple roles, splits them up
		roles := strings.Split(role, ",")

		// iterates over its roles
		for _, r := range roles {
			// based on the role number
			// adds the peer name to the key (the role number) of the role specific map
			roleNumber, err := strconv.Atoi(string(r[len(r)-1]))
			checkIfError(err, "role number failed")

			for _, num := range processNumbers {
				// if roleNumber isn't in processNumbers append it
				if num != roleNumber {
					processNumbers = append(processNumbers, num)
				}
			}

			switch {
			case strings.Contains(r, "proposer"):
				proposerMap[peer_name] = roleNumber

			case strings.Contains(r, "acceptor"):
				acceptorMap[peer_name] = roleNumber
			case strings.Contains(r, "learner"):
				learnerMap[peer_name] = roleNumber
			}
		}

	}

}

func listen() {
	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)
	// fmt.Fprintln(os.Stderr, "Server Running...")
	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")
	defer listener.Close()
	// fmt.Println(os.Stderr, "Listening on ")
	// fmt.Println(os.Stderr, "Waiting for client...")
	for {
		connection, err := listener.Accept()
		checkIfError(err, "Error accepting: ")

		// fmt.Println("client connected")
		remoteAddr := connection.RemoteAddr().String()
		ip, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			fmt.Println("Error splitting host and port:", err)
			return
		}

		hostnames, err := net.LookupAddr(ip)
		if err != nil {
			fmt.Println("Error performing reverse DNS lookup:", err)
			return
		}

		peerName := strings.Split(hostnames[0], ".")
		go receiveValue(connection, peerName[0])

	}
}

// Convert proposal to byte slice

func proposalToBytes(p proposal) []byte {
	return []byte(fmt.Sprintf("%d,%c", p.id, p.value))
}

// Convert byte slice to proposal
func bytesToProposal(data []byte) proposal {
	var p proposal
	fmt.Sscanf(string(data), "%d,%c", &p.id, &p.value)
	return p
}

// set up server
func sendValue(peer string, prop proposal) {
	var SERVER_TYPE = "tcp"

	fmt.Fprintf(os.Stderr, "%s", peer)
	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer+":"+PORT)
	checkIfError(err, "Error resolving address:")

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	checkIfError(err, "Error connecting:")

	proposalBytes := proposalToBytes(prop)

	///send proposal
	_, err = connection.Write(proposalBytes)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	// fmt.Println("Received from ", peer)
	defer connection.Close()
}

func receiveValue(connection net.Conn, peerName string) {
	defer connection.Close()

	//non blocking
	//connection.SetDeadline(time.Time{})

	// Read proposal from the connection
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return
	}

	receivedProposal := bytesToProposal(buffer[:mLen])

	if isProposer {
		// fmt.Printf("[Proposer] Received Proposal: %+v\n", receivedProposal)

		receives_acceptor(receivedProposal, peerName)
	}

	if isAcceptor {
		// fmt.Printf("[Acceptor] Received Proposal: %+v\n", receivedProposal)

		accepting(receivedProposal, peerName)
	}

	if isLearner {
		// fmt.Printf("[Learner] Received Proposal: %+v\n", receivedProposal)

		accept_from_proposer(receivedProposal, peerName)
	}

}

// proposer method propose to acceptors
func prepare(v rune) {
	myProposal := proposal{
		id:    minProposal + 1,
		value: rune(v),
	}

	var processNumbers []int
	for key, value := range proposerMap {
		if key == hostname {
			processNumbers = append(processNumbers, value)
		}
	}

	for peer, value := range acceptorMap {
		for _, val := range processNumbers {
			if val == value {
				fmt.Fprintf(os.Stderr, "[Proposer] Prepare {id: %d, value: %c} to %s\n", myProposal.id, myProposal.value, peer)
				sendValue(peer, myProposal)

			}
		}
	}

}

// acceptors waiting for proposals
// acceptor method accepts first proposal it receives and rejects the rest
func accepting(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Acceptor] [Recv] {id: %d, value: %c} from %s\n", myProposal.id, myProposal.value, peerName)

	if prop.id > minProposal {
		minProposal = prop.id
		acceptedProposal = proposal{
			id:    prop.id,
			value: prop.value,
		}
	}

	send_acceptance(acceptedProposal, peerName)

}

// acceptor method send back to proposers
func send_acceptance(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Acceptor] [Sent] {id: %d, value: %c} to %s\n", myProposal.id, myProposal.value, peerName)
	sendValue(peerName, prop)

}

// proposer method
// receieve response from acceptors
func receives_acceptor(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Proposer] [Recv] {id: %d, value: %c} from %s \n", myProposal.id, myProposal.value, peerName)

	if prop.id == myProposal.id {
		acceptances++
	}

	var processNumbers []int
	for key, value := range proposerMap {
		if key == hostname {
			processNumbers = append(processNumbers, value)
		}
	}

	targetAcceptances := len(acceptorMap)/2 + 1
	fmt.Fprintf(os.Stderr, "%d", targetAcceptances)

	for peer, value := range learnerMap {
		for _, val := range processNumbers {
			if val == value && acceptances == targetAcceptances {
				send_learners(myProposal, peer)
				break

			}
		}
	}

}

// proposer method send chosen value to learners
func send_learners(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Proposer] [Sent]  {id: %d, value: %c} to %s\n", myProposal.id, myProposal.value, peerName)
	sendValue(peerName, prop)
}

// learner method accept from proposer
func accept_from_proposer(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Learner] [Recv] {id: %d, value: %c} to %s\n", myProposal.id, myProposal.value, peerName)

}

func main() {
	// minProposal := 0

	// command line inputs
	file := flag.String("h", "hostsfile.txt", "hosts filename")
	propose_delay := flag.Int("t", 0, "the delay before proposing")
	value := flag.String("v", "", "the value proposed")

	flag.Parse()

	// read hostsfile
	gethosts(*file)
	var v rune
	if utf8.RuneCountInString(*value) == 1 {
		v, _ = utf8.DecodeRuneInString(*value)
		fmt.Printf("Value as rune: %c\n", v)
	}

	duration := time.Duration(*propose_delay) * time.Second

	if isProposer {
		fmt.Fprintln(os.Stderr, "Sending prepare")
		time.Sleep(duration)
		prepare(v)
	}

	listen()

}
