
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

    def match_parentheses(self, tokens, index, open_p, close_p):
        if tokens[index] != open_p:
            raise "match parenthesis, start is not a bracket or parentheses"
        else:
            counting = 1
            for i in range(index + 1, len(tokens)):
                if tokens[i] == open_p:
                    counting += 1
                elif tokens[i] == close_p:
                    counting -= 1
                    if counting == 0:
                        return index, i
            raise "no ending parenthesis"

    def parse_validator(self):
        tokens = self.text.split()
        blocks = []
        paths = []
        for i in range(len(tokens)):
            if tokens[i] == '{':
                blocks.append([self.match_parentheses(tokens,i,'{','}')])
        for block in blocks:
            count = 0
            current_block = tokens[block[0][0]:block[0][1]+1]
            for i in range(0,len(current_block)):
                path = []
                if current_block[i] == 'thread':
                    process_name = current_block[i+1]
                    for j in range(i+2, len(current_block)):
                        process_name += current_block[j]
                        if ')' in current_block[j]:
                            break
                    for j in range(i,len(current_block)):
                        if current_block[j] == '{':
                            f,l = self.match_parentheses(current_block,j,'{','}')
                            current_block.insert(f+1,process_name)
                            paths.append(current_block[f:l+1])
                            break
        print(paths)

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
    replicate_log = """
    Validator ReplicateLog {
        thread leader(*, 1) {
            notice("request_from_client")
            task("broadcast_entry") {
                repeat 2 { send(follower) }
            }
            task("replicate_entry") {
                recv(follower)
            }
            notice("send_back_to_client")
            recv(follower)
        }

        thread follower(*, 2) {
            recv(leader)
            send(leader)
        }
    }
    """
    p2 = Parser(replicate_log)
    p2.parse_validator()
    # parser = Parser(test)
    # parser.parse_validator()

    # y = 0
    # for x in test:
    #     if x == '{':
    #         y += 1
    # print(test.split(' '))

if __name__ == '__main__':
    main()