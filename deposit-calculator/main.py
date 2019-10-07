from flask import Flask, request, render_template

app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def index():
    deposit_return = ''
    if request.method == 'POST':
        deposit_amount  = float(request.form.get('deposit_amount'))
        interest_rate   = float(request.form.get('interest_rate'))
        investment_term = float(request.form.get('investment_term'))
        deposit_return  = calculate_deposit(deposit_amount, interest_rate, investment_term)
    return render_template("deposit_calc.html", deposit_return=deposit_return)

def calculate_deposit(deposit_amount, interest_rate, investment_term):
    return round((deposit_amount * interest_rate/100 * investment_term/12), 2)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
