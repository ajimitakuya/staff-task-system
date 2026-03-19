from flask import Flask, render_template_string, redirect, url_for, request

app = Flask(__name__)

# 擬似的なデータベース（決済状態を保持）
payment_status = {"completed": False}
UNLOCK_CODE = "7788"  # 箱の暗証番号

# --- 1. 商品ページ（QRを読み込んだ後に表示される画面） ---
@app.route('/')
def index():
    return render_template_string('''
        <h1>📦 商品ボックス #001</h1>
        <p>中身：高級焼肉セット（イメージ）</p>
        <img src="https://via.placeholder.com/200" alt="商品の写真"><br><br>
        <form action="/pay" method="post">
            <button type="submit" style="padding:15px; background:red; color:white;">PayPayで決済する</button>
        </form>
    ''')

# --- 2. 決済ページ（PayPayの画面を模したもの） ---
@app.route('/pay', methods=['POST'])
def pay():
    return render_template_string('''
        <h1>📱 PayPay決済画面（シミュレーション）</h1>
        <p>金額：1,000円</p>
        <button onclick="location.href='/callback?status=success'" style="padding:20px;">【デバッグ用】決済を完了させる</button>
    ''')

# --- 3. 決済後の戻り先（完了判定をして番号を表示） ---
@app.route('/callback')
def callback():
    status = request.args.get('status')
    if status == 'success':
        payment_status['completed'] = True
        return f'''
            <h1>✅ 決済が完了しました！</h1>
            <p>箱の暗証番号は <strong>{UNLOCK_CODE}</strong> です。</p>
            <a href="/">トップに戻る</a>
        '''
    else:
        return '<h1>❌ 決済に失敗しました</h1>'

if __name__ == '__main__':
    # ゆーのPCのIPアドレスで公開（スマホから見る用）
    app.run(host='0.0.0.0', port=5000, debug=True)