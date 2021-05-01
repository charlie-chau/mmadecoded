from flask import Flask
from flask import request
from app.backend.odds_broker import OddsBroker
from app.backend.odds_compiler import OddsCompiler
import json

app = Flask(__name__)


@app.route('/matchup_odds')
def get_matchup_odds():
    sources = request.args['sources'].split(',')

    print('Sources requested: {}'.format(sources))

    broker = OddsBroker(sources)

    return broker.retrieve()


@app.route('/compiled_odds', methods=['POST'])
def get_compiled_odds():
    data = request.get_json()
    print(data)

    compiler = OddsCompiler(data['models'], data['fights'])

    result = compiler.get_compiled_odds()

    print(result)
    # print(json.dumps(result, indent=4))

    return result


if __name__ == '__main__':
    app.run(threaded=False)
