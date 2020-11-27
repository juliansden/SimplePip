
KEYWORDS = [
    'thread', 'maybe', 'task', 
    'send', 'recv', 'repeat', 
    'repeat between', 'and', 'or', 
    'xor', 'branch', 'notice'
    ]

class Parser():
    def __init__(self, text):
        super(Parser, self).__init__()
        self.text = text

    def match_parentheses(self, tokens, index):
        if tokens[index] != '{':
            raise "match parenthesis, start is not {"
        else:
            counting = 1
            for i in range(index + 1, len(tokens)):
                if tokens[i] == '{':
                    counting += 1
                elif tokens[i] == '}':
                    counting -= 1
                    if counting == 0:
                        return index, i
            raise "no ending parenthesis"

    def parse_validator(self):
        tokens = self.text.split()
        blocks = []
        for i in range(len(tokens)):
            if tokens[i] == '{':
                blocks.append([self.match_parentheses(tokens,i)])
        for block in blocks:
            count = 0
            current_block = tokens[block[0][0]:block[0][1]]
            for keyword in KEYWORDS:
                for token in current_block:
                    if keyword in token:
                        count += 1
            if count == 1:
                print(current_block)

class Recognizer():
    """docstring for Recognizer"""
    def __init__(self, parser, validator):
        super(Recognizer, self).__init__()
        self.parser = parser
        self.validator = validator

    """Generate parse tree from validator."""
    def generate_parse_tree(self):
        pass     
        
    def is_expected(self,annotation):
        pass

def main():
    test = """
    Validator Heartbeat { 
        thread leader(*, 1) { 
            task("heartbeat") { 
                repeat 2 { send(follower) } 
            } 
        } 
        thread follower(*, 2) { 
            maybe {  recv(leader)  } 
        } 
    } 
    """

    parser = Parser(test)
    parser.parse_validator()

    # y = 0
    # for x in test:
    #     if x == '{':
    #         y += 1
    # print(test.split(' '))

if __name__ == '__main__':
    main()