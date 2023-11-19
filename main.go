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
	id       int
	value    rune
	serverId string
}

var (
	hosts            []string
	PORT             = "4950"
	isProposer       bool
	isAcceptor       bool
	isLearner        bool
	minProposal      = 0
	acceptorMap      map[string][]int
	proposerMap      map[string][]int
	learnerMap       map[string][]int
	acceptedProposal proposal
	acceptances      int
	myProposal       proposal
	processNumbers   []int
	hostname         string
	proposalVotes    map[proposal]int
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
	// fmt.Fprintf(os.Stderr, "hostname: %s\n", hostname)
	checkIfError(err, "Error getting hostname:")

	acceptorMap = make(map[string][]int)
	proposerMap = make(map[string][]int)
	learnerMap = make(map[string][]int)

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
				proposerMap[peer_name] = append(proposerMap[peer_name], roleNumber)

			case strings.Contains(r, "acceptor"):
				acceptorMap[peer_name] = append(acceptorMap[peer_name], roleNumber)
			case strings.Contains(r, "learner"):
				learnerMap[peer_name] = append(learnerMap[peer_name], roleNumber)
			}
		}

		// fmt.Fprintln(os.Stdout, proposerMap)
		// fmt.Fprintln(os.Stdout, acceptorMap)
		// fmt.Fprintln(os.Stdout, learnerMap)

	}

}

func listen(listener net.Listener) {
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
	return []byte(fmt.Sprintf("%d,%c,%s", p.id, p.value, p.serverId))
}

// Convert byte slice to proposal
func bytesToProposal(data []byte) proposal {
	var p proposal
	fmt.Sscanf(string(data), "%d,%c,%s", &p.id, &p.value, &p.serverId)
	return p
}

// set up server
func sendValue(peer string, prop proposal) {
	var SERVER_TYPE = "tcp"

	addr, err := net.ResolveTCPAddr(SERVER_TYPE, peer+":"+PORT)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	connection, err := net.DialTCP(SERVER_TYPE, nil, addr)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}

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
	myProposal = proposal{
		id:       minProposal + 1,
		value:    v,
		serverId: hostname,
	}
	proposalVotes = make(map[proposal]int)
	proposalVotes[myProposal] = 0
	processNumbers := proposerMap[hostname] // Use the proposerMap directly for the current host
	// fmt.Fprintln(os.Stdout, acceptorMap)

	for peer, value := range acceptorMap {
		for _, val := range value {
			if contains(processNumbers, val) {
				fmt.Fprintf(os.Stderr, "[Proposer] [Prepare] {id: %d, value: %c, serverId: %s} to %s\n", myProposal.id, myProposal.value, myProposal.serverId, peer)
				sendValue(peer, myProposal)
			}
		}
	}
}

func contains(list []int, value int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

// acceptors waiting for proposals
// acceptor method accepts first proposal it receives and rejects the rest
func accepting(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Acceptor] [Recv] {id: %d, value: %c, serverId: %s} from %s\n", prop.id, prop.value, prop.serverId, peerName)
	// fmt.Fprintf(os.Stderr, "prop id: %d\n", prop.id)
	if prop.id > minProposal {
		acceptedProposal = proposal{
			id:       prop.id,
			value:    prop.value,
			serverId: prop.serverId,
		}
		minProposal = prop.id

	}

	send_acceptance(acceptedProposal, prop.serverId)

}

// acceptor method send back to proposers
func send_acceptance(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Acceptor] [Sent] {id: %d, value: %c, serverId: %s} to %s\n", prop.id, prop.value, prop.serverId, peerName)
	sendValue(peerName, prop)

}

// proposer method
// receieve response from acceptors
func receives_acceptor(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Proposer] [Recv] {id: %d, value: %c} from %s \n", prop.id, prop.value, peerName)

	proposalVotes[prop]++

	//fmt.Fprintf(os.Stderr, "accept")

	for propv, count := range proposalVotes {
		if count >= len(acceptorMap)/2+1 {
			// Proposal has a majority, send it to matching learners
			//send_learners(prop, learnerMap)
			for peer, values := range learnerMap {
				// if identifier is same as host
				if contains(values, proposerMap[hostname][0]) {
					send_learners(propv, peer)
				}

			}
			fmt.Fprintf(os.Stderr, "[Proposer] [Accept] {id: %d, value: %c} \n", propv.id, propv.value)

		}
	}

}

// proposer method send chosen value to learners
func send_learners(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Proposer] [Sent]  {id: %d, value: %c} to %s\n", prop.id, prop.value, peerName)
	sendValue(peerName, prop)
}

// learner method accept from proposer
func accept_from_proposer(prop proposal, peerName string) {
	fmt.Fprintf(os.Stderr, "[Learner] [Recv] {id: %d, value: %c} to %s\n", prop.id, prop.value, peerName)
	fmt.Fprintf(os.Stderr, "[Learner] [Accept] {id: %d, value: %c} to %s\n", prop.id, prop.value, peerName)

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
		// fmt.Printf("Value as rune: %c\n", v)
	}

	duration := time.Duration(*propose_delay) * time.Second
	var (
		SERVER_HOST = hostname
		SERVER_PORT = "4950"
		SERVER_TYPE = "tcp"
	)

	// fmt.Fprintln(os.Stderr, "Server Running...")
	listener, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+SERVER_PORT)
	checkIfError(err, "Error listening:")

	go listen(listener)

	if isProposer {
		// fmt.Fprintln(os.Stderr, "Sending prepare")
		// give time for other peers to get online
		time.Sleep(2 * time.Second)
		time.Sleep(duration)
		prepare(v)
	}

	select {}

}
