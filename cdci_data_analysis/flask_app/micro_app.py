from flask import Flask, jsonify, abort

micro_service = Flask("micro_service")

#in memory data store (in practice this is pulled from a DB)
class Address():
    def __init__(self, ID, number, line_1, line_2, post_code):
        self.ID = ID
        self.number = number
        self.line_1 = line_1
        self.line_2 = line_2
        self.post_code = post_code

add1 = Address(1, 2, "North Street", "Python City", "PY11 0PY")
add2 = Address(2, 21, "South Street", "Python City", "PY12 5PY")

addresses = [add1,add2]

@micro_service.route('/address_book/api/v1.0/addresses', methods=['GET'])
def get_addressess():
    return jsonify({'addresses': [add.__dict__ for add in addresses ]})

def run_micro_service(conf,debug=False,threaded=False):
    micro_service.config['conf'] = conf
    #if conf.sentry_url is not None:
    print('conf micro',micro_service.config['conf'])
    #sentry = Sentry(app, dsn=conf.sentry_url)
    micro_service.app.run(host=conf.microservice_url, port=conf.microservice_port, debug=debug,threaded=threaded)

#if __name__ == '__main__':
#    micro_service.run(host="localhost", port=12345)