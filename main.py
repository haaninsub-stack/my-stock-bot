import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pykrx import stock
from datetime import datetime, timedelta  # timedelta 추가
import time
import os
import json
import warnings

# 경고 메시지 무시
warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# 1. 구글 시트 연결 설정
# ---------------------------------------------------------
def connect_google_sheet():
    # GitHub Secrets에 저장된 키(JSON)를 가져옵니다
    json_key = os.environ.get('GOOGLE_SHEET_KEY')
    if not json_key:
        print("Error: 구글 시트 키가 없습니다. GitHub Secrets 설정을 확인하세요.")
        return None
        
    key_dict = json.loads(json_key)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    client = gspread.authorize(creds)
    
    # 시트 이름이 구글 드라이브에 있는 파일명과 똑같아야 합니다!
    return client.open('StockData_Gems').sheet1

# ---------------------------------------------------------
# 2. 네이버 금융 크롤링 (재무제표, 추정치)
# ---------------------------------------------------------
def get_naver_financials(code, current_price):
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    data = {}
    
    try:
        res = requests.get(url, headers=headers)
        # '최근 연간 실적'이라는 단어가 포함된 표를 찾습니다
        dfs = pd.read_html(res.text, match='최근 연간 실적')
        if not dfs:
            return None
            
        df = dfs[0]
        
        # 표의 머리글(컬럼) 정리
        df.set_index(df.columns[0], inplace=True)
        df.columns = df.columns.droplevel(0)
        
        # 날짜가 적힌 컬럼만 골라내기 (2024.12 등)
        cols = [c for c in df.columns if len(str(c)) > 3]
        
        # (E)가 없는 건 '확정 실적', (E)가 있는 건 '추정치'
        confirmed_cols = [c for c in cols if '(E)' not in c]
        estimate_cols = [c for c in cols if '(E)' in c]
        
        # [A] 과거 실적 (최근 4분기)
        # 데이터가 4개보다 적으면 있는 만큼만 가져옴
        recent_cols = confirmed_cols[-4:] if len(confirmed_cols) >= 4 else confirmed_cols
        
        for i, col in enumerate(recent_cols):
            q_num = i + 1 # 1분기, 2분기...
            
            # 매출액, 영업이익 가져오기 (없으면 0)
            try: data[f'매출_과거{q_num}'] = float(df.loc['매출액', col])
            except: data[f'매출_과거{q_num}'] = 0
                
            try: data[f'영업이익_과거{q_num}'] = float(df.loc['영업이익', col])
            except: data[f'영업이익_과거{q_num}'] = 0
            
            # 배당수익률 계산 (주당배당금 / 현재가)
            try:
                dps = df.loc['주당배당금(원)', col]
                if pd.isna(dps) or dps == '-' or float(dps) == 0:
                    data[f'배당수익률_과거{q_num}'] = 0.0
                else:
                    data[f'배당수익률_과거{q_num}'] = round((float(dps) / current_price) * 100, 2)
            except: 
                data[f'배당수익률_과거{q_num}'] = 0.0
                
            # 배당성향
            try:
                payout = df.loc['배당성향(%)', col]
                data[f'배당성향_과거{q_num}'] = 0.0 if (pd.isna(payout) or payout == '-') else float(payout)
            except: 
                data[f'배당성향_과거{q_num}'] = 0.0

        # [B] 전년 대비 성장률 (YoY) - 가장 최근 분기와 1년 전 분기 비교
        if len(confirmed_cols) >= 5:
            last = confirmed_cols[-1]
            prev = confirmed_cols[-5]
            try:
                cur_op = float(df.loc['영업이익', last])
                prev_op = float(df.loc['영업이익', prev])
                # 전년도 이익이 0이면 계산 불가
                if prev_op == 0: data['영업이익_YoY(%)'] = 0.0
                else: data['영업이익_YoY(%)'] = round(((cur_op - prev_op) / abs(prev_op)) * 100, 2)
            except: data['영업이익_YoY(%)'] = 0.0
        else:
            data['영업이익_YoY(%)'] = 0.0

        # [C] 미래 추정치 (다음 1분기 컨센서스)
        if estimate_cols:
            next_est = estimate_cols[0] # 가장 가까운 미래
            try: data['추정_매출_다음분기'] = float(df.loc['매출액', next_est])
            except: data['추정_매출_다음분기'] = 0
            
            try: data['추정_영업이익_다음분기'] = float(df.loc['영업이익', next_est])
            except: data['추정_영업이익_다음분기'] = 0
        else:
            data['추정_매출_다음분기'] = 0
            data['추정_영업이익_다음분기'] = 0
            
        return data

    except Exception:
        return {}

# ---------------------------------------------------------
# 3. 메인 실행 함수
# ---------------------------------------------------------
def main():
    print("데이터 수집 시작...")
    # 오늘 날짜 또는 어제 날짜 사용 (주말/공휴일 대비)
    # today = datetime.now().strftime("%Y%m%d")
    today = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")  # 어제 날짜 사용
    
    # 코스피, 코스닥 종목 코드 다 가져오기 (시간 문제로 10개만 테스트)
    # kospi = stock.get_market_ticker_list(today, market="KOSPI")
    # kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
    # tickers = kospi + kosdaq
    tickers = stock.get_market_ticker_list(today, market="KOSPI")[:10] # KOSPI 10개 종목만 테스트
    
    print(f"총 {len(tickers)}개 종목 수집 시작")
    
    all_data = []
    
    for idx, code in enumerate(tickers):
        # 100개마다 진행상황 표시
        if idx % 100 == 0: print(f"{idx}개 완료...")
            
        try:
            # 기본 정보
            name = stock.get_market_ticker_name(code)
            print(f"Processing {code}: {name}")  # 종목명 출력
            df_p = stock.get_market_ohlcv(today, today, code)
            
            if df_p.empty:
                current_price = 0
            else:
                current_price = int(df_p['종가'].iloc[-1])
            
            # 주가가 0원이면 배당 계산 시 에러나므로 1원으로 임시 처리
            calc_price = current_price if current_price > 0 else 1
            
            # 상장주식수
            cap = stock.get_market_cap(today, today, code)
            shares = int(cap['상장주식수'].iloc[-1]) if not cap.empty else 0
            
            # 네이버 금융 데이터 가져오기
            fin_data = get_naver_financials(code, calc_price)
            
            # 한 줄 데이터 완성
            row = {
                '종목코드': f"A{code}", # 엑셀에서 0 빠짐 방지
                '종목명': name,
                '현재가': current_price,
                '상장주식수': shares,
                **fin_data
            }
            all_data.append(row)
            
            # 너무 빠르면 차단당하니 0.1초 휴식
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error processing {code}: {e}") # 에러 로그 출력
            row = {
                '종목코드': f"A{code}",
                '종목명': "ERROR",
                '현재가': 0,
                '상장주식수': 0,
                '매출_과거1': 0,
                '영업이익_과거1': 0,
                '배당수익률_과거1': 0,
                '배당성향_과거1': 0,
                '매출_과거2': 0,
                '영업이익_과거2': 0,
                '배당수익률_과거2': 0,
                '배당성향_과거2': 0,
                '매출_과거3': 0,
                '영업이익_과거3': 0,
                '배당수익률_과거3': 0,
                '배당성향_과거3': 0,
                '매출_과거4': 0,
                '영업이익_과거4': 0,
                '배당수익률_과거4': 0,
                '배당성향_과거4': 0,
                '매출_YoY(%)': 0,
                '영업이익_YoY(%)': 0,
                '추정_매출_다음분기': 0,
                '추정_영업이익_다음분기': 0
            }
            all_data.append(row)
            continue
            
    # 구글 시트에 저장
    print("구글 시트에 저장 중...")
    df = pd.DataFrame(all_data)
    df.fillna(0, inplace=True) # 빈 값은 0으로 채움
    
    sheet = connect_google_sheet()
    if sheet:
        sheet.clear() # 기존 내용 지우기
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("완료!")
    else:
        print("구글 시트 연결 실패")

if __name__ == "__main__":
    main()
