import tokenize
import json
import re
from abc import ABC, abstractmethod 

# get next token except "\n"
def next_token(tokens):
    try:
        token = next(tokens)
        if token.type == tokenize.NL:
            return next_token(tokens)
        return token
    except StopIteration:
        return None

class Expectation(ABC):
    def check(self, events):
        pass

class Validator(Expectation):
    def __init__(self, tokens):
        self.name = next_token(tokens).string
        assert next_token(tokens).string == '{'
        self.children = []
        token = next_token(tokens)
        while token and token.string != '}':
            # assert token.string == 'thread'clea
            self.children.append(Thread(tokens))
            token = next_token(tokens)

    def check(self, path_instance):
        res = True
        for pid, events in path_instance.items():
            # TODO: this logic can be better
            match = False
            for thread in self.children:
                if thread.check(events):
                    print(f'{pid} matches {thread.name}')
                    match = True
            res = res and match
        return res

class Thread(Expectation):
    def __init__(self, tokens):
        self.name = next_token(tokens).string
        self.children = []
        next_token(tokens)
        next_token(tokens)
        next_token(tokens)
        self.count = int(next_token(tokens).string)
        next_token(tokens)
        assert next_token(tokens).string == '{'
        token = next_token(tokens)
        while token and token.string != '}':
            self.children.append(dispatch[token.string](tokens))
            token = next_token(tokens)

    def dfs(self, events, event_idx, child_index):
        if child_index == len(self.children) and event_idx == len(events):
            return True
        if child_index == len(self.children) or event_idx == len(events):
            return False
        matched = self.children[child_index].check(events[event_idx:])
        for i in matched:
            if self.dfs(events, event_idx + i, child_index + 1):
                return True
        return False

    def check(self, events):
        # events here is a list
        return self.dfs(events, 0, 0)

class Task(Expectation):
    def __init__(self, tokens):
        self.children = []
        token = next_token(tokens)
        self.name = next_token(tokens).string[1:-1]
        next_token(tokens)
        assert next_token(tokens).string == '{'
        token = next_token(tokens)
        while token and token.string != '}':
            self.children.append(dispatch[token.string](tokens))
            token = next_token(tokens)
            
    def dfs(self, events, event_idx, child_index):
        # If iterate through all events and all expectation
        if child_index == len(self.children) and event_idx == len(events):
            return True
        # If there are still events or expectations left
        if child_index == len(self.children) or event_idx == len(events):
            return False
        matched = self.children[child_index].check(events[event_idx:])
        for i in matched:
            if self.dfs(events, event_idx + i, child_index + 1):
                return True
        return False

    def check(self, events):
        if events[0]['type'] == 'task' and events[0]['name'] == self.name:
            if self.dfs(events[0]['logs'], 0, 0):
                return [1]
        return []

class Send(Expectation):
    def __init__(self, tokens):
        assert next_token(tokens).string == '('
        self.receiver = next_token(tokens).string
        assert next_token(tokens).string == ')'

    def check(self, events):
        if events[0]['type'] == 'send':
            return [1]
        else:
            return []

class Receive(Expectation):
    def __init__(self, tokens):
        assert next_token(tokens).string == '('
        self.sender = next_token(tokens).string
        assert next_token(tokens).string == ')' 

    def check(self, events):
        if events[0]['type'] == 'receive':
            return [1]
        else:
            return []

class Notice(Expectation):
    def __init__(self, tokens):
        assert next_token(tokens).string == '('
        token = next_token(tokens)
        if token.string == '/':
            token = next_token(tokens)
            regex = ''
            while token and token.string != '/':
                regex = regex + token.string
                token = next_token(tokens)
            self.detail = re.compile(regex)
        else:
            self.detail = re.compile(token.string[1:-1])
        assert next_token(tokens).string == ')' 

    def check(self, events):
        if events[0]['type'] == 'notice' and self.detail.match(events[0]['detail']):
            return [1] 
        else:
            return []

class Repeat(Expectation):
    def __init__(self, tokens):
        token = next_token(tokens)
        if token.string == "between":
            self.lo = int(next_token(tokens).string)
            assert next_token(tokens).string == 'and'
            self.hi = int(next_token(tokens).string)
            if self.hi < self.lo:
                raise 'repeat between x and y, x should not be larger than y'
        else:
            self.lo = int(token.string)
            self.hi = self.lo
        self.children = []
        assert next_token(tokens).string == '{'
        token = next_token(tokens)
        while token and token.string != '}':
            self.children.append(dispatch[token.string](tokens))
            token = next_token(tokens)

    def dfs(self, events, event_idx, child_idx, repeated_times, result):
        if child_idx % len(self.children) == 0:
            repeated_times += 1
            if self.lo <= repeated_times <= self.hi:
                result.append(event_idx)
            child_idx = 0
        if repeated_times >= self.hi or event_idx == len(events):
            return
        matched = self.children[child_idx].check(events[event_idx:])
        for i in matched:
            self.dfs(events, event_idx + i, child_idx + 1, repeated_times, result)

    def check(self, events):
        result = []
        self.dfs(events, 0, 0, -1, result)
        return result

class Maybe(Expectation):
    def __init__(self, tokens):
        self.children = []
        next_token(tokens)
        token = next_token(tokens)
        while token and token.string != '}':
            self.children.append(dispatch[token.string](tokens))
            token = next_token(tokens) 

    def dfs(self, events, event_idx, child_index):
        if child_index == len(self.children):
            return event_idx
        elif event_idx == len(events):
            return None
        matched = self.children[child_index].check(events[event_idx:])
        for i in matched:
            index = self.dfs(events, event_idx + i, child_index + 1)
            if index:
                return index
        return None

    def check(self, events):
        idx = self.dfs(events, 0, 0)
        if idx:
            return [0, idx]
        else:
            return [0]

class Xor(Expectation):
    def __init__(self, tokens):
        pass

    def check(self, events):
        pass

class Any(Expectation):
    def __init__(self, tokens):
        self.children = []
        next_token(tokens)
        token = next_token(tokens)
        while token and token.string != '}':
            self.children.append(dispatch[token.string](tokens))
            token = next_token(tokens) 
    
    def dfs(self, events, event_idx, child_idx, result):
        if child_idx % len(self.children) == 0:
            result.append(event_idx)
            child_idx = 0
        if event_idx == len(events):
            return
        matched = self.children[child_idx].check(events[event_idx:])
        for i in matched:
            self.dfs(events, event_idx + i, child_idx + 1, result)

    def check(self, events):
        result = []
        self.dfs(events, 0, 0, result)
        return result

dispatch = {"task": Task, "send": Send, "recv": Receive, "notice": Notice, "repeat": Repeat, "maybe": Maybe, "xor": Xor, "any": Any}

def main():
    with open('expectations_raft/replicate_log') as f:
    # with open('pip_example') as f:
        tokens = tokenize.generate_tokens(f.readline)
        validators = []
        for token in tokens:
            if token.string == 'validator':
                v = Validator(tokens)
                validators.append(v)
    # with open('pip_example_instance') as f:
    with open('path_instance_raft/a_1') as f:
        p = json.loads(f.read())
        for v in validators:
            if v.check(p):
                print(f'matches {v.name}')
            else:
                print(f'doesn\'t match {v.name}')
    return

if __name__ == '__main__':
    main()
