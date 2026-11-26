# 꼰대 연구소 (kkondae-lab)

꼰대력 테스트 — 8가지 꼰대 유형 중 나는 어디에 해당하는지 알아보는 36문항 테스트입니다.
결과를 상대와 비교할 수 있는 2인 채팅(방 코드 / 랜덤 매칭) 기능이 포함되어 있습니다.

## 실행

Node.js 18 이상이 필요합니다. 별도 설치할 패키지와 API 키는 없습니다.

```
node server.js
```

실행 후 http://localhost:2500 에 접속하세요.

## 환경 변수 (선택)

| 이름 | 기본값 | 설명 |
|---|---|---|
| `PORT` | `2500` | 서버 포트 |
| `HOST` | `127.0.0.1` | 바인딩 주소 (`PORT`가 지정된 배포 환경에서는 자동으로 `0.0.0.0`) |

## 배포 (Render)

1. GitHub에 push 후 [Render](https://render.com) → New → Web Service 에서 이 repo 연결
2. Runtime: `Node`, Build Command: 비워두기(또는 `npm install`), Start Command: `npm start`
3. Deploy 후 발급된 `https://....onrender.com` 주소로 접속

> 무료 플랜은 일정 시간 접속이 없으면 잠들고, 재시작하면 `data.json`(방/채팅 기록)이 초기화됩니다.

**배포 주소:** https://kkondae-lab.onrender.com

## 구성

- `kkondae.html` — 테스트 화면 (문항, 결과, 채팅 UI)
- `server.js` — 정적 파일 + 방/채팅 API (변경 알림은 SSE)
- `data.json` — 서버 실행 중 자동 생성되는 방 데이터 (git 제외)
