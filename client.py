import grpc

import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2


def ensure_connected(state):
    if not state['node_addr']:
        return "No address to connect to was specified", state
    channel = grpc.insecure_channel(state['node_addr'])
    state['stub'] = pb2_grpc.RaftNodeStub(channel)
    return None, state


def cmd_connect(node_addr, state):
    state['node_addr'] = node_addr
    return "", state


def cmd_getleader(state):
    (err_msg, state1) = ensure_connected(state)
    if err_msg:
        return (err_msg, state1)
    resp = state1['stub'].GetLeader(pb2.EmptyMessage())
    return f"{resp.leader_id} {resp.address}", state1


def cmd_suspend(duration, state):
    (err_msg, state1) = ensure_connected(state)
    if err_msg:
        return (err_msg, state1)
    state1['stub'].Suspend(pb2.SuspendRequest(period=int(duration)))
    return "", state1


def cmd_setval(key, value, state):
    (err_msg, state1) = ensure_connected(state)
    if err_msg:
        return (err_msg, state1)
    state1['stub'].SetVal(pb2.SetRequest(key=key, value=value))
    return "", state1


def cmd_getval(key, state):
    (err_msg, state1) = ensure_connected(state)
    if err_msg:
        return (err_msg, state1)
    resp = state1['stub'].GetVal(pb2.GetRequest(key=key))
    if resp.success:
        return f"{resp.value}", state1
    return "None", state1


def exec_cmd(line, state):
    parts = line.split()
    if parts[0] == 'connect':
        return cmd_connect(f"{parts[1]}:{parts[2]}", state)
    elif parts[0] == 'getleader':
        return cmd_getleader(state)
    elif parts[0] == 'suspend':
        return cmd_suspend(int(parts[1]), state)
    elif parts[0] == 'setval':
        return cmd_setval(parts[1], parts[2], state)
    elif parts[0] == 'getval':
        return cmd_getval(parts[1], state)
    elif parts[0] == 'quit':
        state['working'] = False
        return "The client ends", state
    else:
        return f"Unknown command {parts[0]}", state


if __name__ == '__main__':
    # channel = grpc.insecure_channel(f'127.0.0.1:{registry_port}')
    state = {'working': True, 'node_addr': None, 'stub': None}
    try:
        while state['working']:
            line = input("> ")
            output, new_state = exec_cmd(line, state)
            if output:
                print(output)
            state = new_state
    except KeyboardInterrupt:
        print("Terminating")
