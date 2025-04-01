const express = require('express');
const oracledb = require('oracledb');
const mssql = require('mssql');
const xmlbuilder = require('xmlbuilder');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const cron = require('node-cron');

// Config 파일 불러오기
const sqlQueries = require('./public/config/sqlQueries.json');
const apiConfig = require('./public/config/apiConfig.json');
const databaseConfig = require('./public/config/databaseConfig.json');
const columnsConfig = require('./public/config/columns.json');
const mssqlSP = require('./public/config/mssqlSP.json');
const oracleCol = require('./public/config/oracleCol.json');
const configUrl = require('./public/config/configUrl.json');
const statusConfigPath = path.join(__dirname, './public/config/statusConfig.json');

const app = express();
const startTime = Date.now();

// 날짜 형식: 현재 시간을 한국시간으로 변환 (ISO 8601 형식)
function formatToKST(date) {
  const kstOffset = 9 * 60; // 9시간의 분 단위 오프셋
  const kstDate = new Date(date.getTime() + kstOffset * 60 * 1000);

  return kstDate.toISOString().replace('Z', '+09:00');
}

// 현재 시간을 한국시간으로 설정
let lastCheckedTime = formatToKST(new Date());

// JSON 파일을 비동기적으로 읽기
let statusCodes;
try {
  const data = fs.readFileSync(statusConfigPath, 'utf8');
  statusCodes = JSON.parse(data);
} catch (err) {
  console.error('Error loading status config:', err);
  statusCodes = {}; // 실패할 경우 빈 객체로 설정
}

// JSON 형식의 요청 바디를 파싱하는 미들웨어 사용
app.use(express.json());

// 정적 파일을 서빙할 디렉토리 설정
app.use(express.static('public'));

// Oracle DB 연결 함수
async function connectOracle(kstDate) {
  let connection;
  try {
    connection = await oracledb.getConnection({
      user: databaseConfig.oracle.user,
      password: databaseConfig.oracle.password,
      connectString: databaseConfig.oracle.connectString
    });

    // 동적 컬럼 목록 생성
    const columns = Object.keys(oracleCol).map(key => oracleCol[key]).join(', ');

    // 동적으로 생성된 컬럼을 쿼리 문자열에 반영
    const query = sqlQueries.oracle.getSalesData.query.replace(':columns', columns);

    // KST로 변환된 시간 파라미터 전달
    const result = await connection.execute(
      query,
      { kstDate: kstDate, lastCheckedTime: lastCheckedTime }  // KST로 변환된 시간 사용
    );

    // 결과 반환
    if (result.rows && result.rows.length > 0) {
      return result.rows;
    } else {
      console.log('No new data returned from Oracle DB.');
      return []; // 데이터가 없을 경우 빈 배열 반환
    }
  } catch (err) {
    console.error('Oracle DB Connection Error:', err);
    throw err;
  } finally {
    if (connection) {
      try {
        await connection.close();  // 연결 종료
      } catch (err) {
        console.error('Error closing connection:', err);
      }
    }
  }
}

// MSSQL 연결 함수 (프로시저 호출)
async function callMSSQLProcedure(data) {
  try {
    const pool = await mssql.connect(databaseConfig.mssql);

    const batchSize = 1000;
    const procConfig = mssqlSP.mssql.storedProcedure;

    // 배치 크기만큼 데이터를 프로시저에 전달하는 로직
    for (const [index, row] of data.entries()) {
      let request = pool.request();

      procConfig.parameters.forEach((param, paramIndex) => {
        const paramValue = row[paramIndex];

        // 데이터에 맞는 타입으로 MSSQL 파라미터 설정
        if (param.type === "mssql.VarChar") {
          request.input(param.name, mssql.VarChar, paramValue);
        } else if (param.type === "mssql.Date") {
          request.input(param.name, mssql.Date, paramValue);
        } else if (param.type === "mssql.Decimal") {
          request.input(param.name, mssql.Decimal, paramValue);
        } else if (param.type === "mssql.DateTime") {
          request.input(param.name, mssql.DateTime, paramValue);
        }
      });

      // 프로시저 호출
      await request.execute(procConfig.name);
      console.log(`Executed stored procedure for row ${index + 1}`);

      if ((index + 1) % batchSize === 0 || index + 1 === data.length) {
        console.log(`Processed ${index + 1} rows`);
      }
    }

    console.log('Data processed with MSSQL procedure');
  } catch (err) {
    console.error('Error executing MSSQL procedure:', err);
  }

  const endTime = Date.now();
  const elapsedTime = endTime - startTime;
  console.log(`MSSQL procedure execution time: ${elapsedTime / 1000} seconds`);
}

// GET 요청 처리 (XML 응답)
app.get(apiConfig.getDataEndpoint, async (req, res) => {
  try {
    const data = await connectOracle(lastCheckedTime);  // Oracle DB에서 데이터 가져오기

    if (data.length === 0) {
      res.status(404).send('No data found');  // 데이터가 없으면 404 응답
      return;  // 응답 후 더 이상 실행되지 않도록 return
    }

    // XML 응답 생성
    const xml = xmlbuilder.create('response', { 
      encoding: 'UTF-8', 
      standalone: 'yes' // XML 선언을 포함하도록 설정
    })
      .ele('message', 'Data fetched successfully from Oracle DB')
      .up()
      .ele('data');  // data 엘리먼트 시작

    // 데이터 배열을 XML로 변환
    data.forEach(row => {
      const item = xml.ele('item');

      // 각 데이터 항목에 대해 statusCode 설정
      const statusCode = row.statusCode || "0000";  // 예시로 row.statusCode로 상태코드 처리
      const statusMsg = statusCodes[statusCode] || "알 수 없는 오류";  // 상태 코드에 따른 메시지 가져오기

      // 상태 코드와 메시지를 XML에 추가
      item.ele('status')
        .ele('code', statusCode).up()
        .ele('message', statusMsg).up()
      .up();  // status 태그 마무리

      // config에서 정의한 컬럼을 기반으로 동적으로 XML 요소 생성
      columnsConfig.oracle.salesColumns.forEach((col, index) => {
        const columnValue = row[index];  // 데이터에서 해당 값 가져오기
        if (columnValue !== undefined && columnValue !== null) {
          item.ele(col.xmlTag, columnValue);  // 동적으로 XML 태그 생성
        }
      });

      item.up();
    });

    // XML 마무리 및 pretty 출력
    let xmlString = xml.end({ pretty: true });

    // XML을 응답으로 전송
    res.set('Content-Type', 'application/xml');
    res.status(200).send(xmlString);  // XML 응답 반환

    // 데이터가 있으면 MSSQL 프로시저 호출
    await callMSSQLProcedure(data);

  } catch (err) {
    if (!res.headersSent) {  // 응답이 이미 보내졌는지 확인
      res.status(500).json({
        message: 'Error fetching data from Oracle DB',
        error: err.message
      });
    }
  }
});

// 주기적으로 Oracle DB 데이터를 가져오고 처리하는 cron 작업
cron.schedule('*/20 * * * * *', async () => {
  console.log('Fetching data from Oracle DB and generating XML...');

  try {
    const kstDate = formatToKST(new Date());  // 새로운 KST 시간 생성
    const data = await connectOracle(kstDate);  // Oracle DB에서 데이터 가져오기

    if (data && data.length > 0) {
      console.log('Data fetched from Oracle DB:', data);

      // 데이터가 있으면 MSSQL 프로시저 호출
      await callMSSQLProcedure(data);

      // 마지막 체크 시간을 업데이트
      lastCheckedTime = kstDate; // KST로 현재 시간 업데이트
    } else {
      console.log('No new data to fetch from Oracle DB.');
    }
  } catch (err) {
    console.error('Error during scheduled task:', err);
  }
});

// POST 요청 처리
app.post(apiConfig.submitDataEndpoint, (req, res) => {
  const receivedData = req.body;
  console.log('Received POST data:', receivedData);

  res.status(200).json({
    message: 'Data received successfully',
    data: receivedData
  });
});

// 서버 시작 후 /api/getData 자동 호출
app.listen(`${configUrl.port}`, async () => {
  console.log(`Server running at ${configUrl.serverUrl}:${configUrl.port}`);

  try {
    await axios.get(`${configUrl.serverUrl}:${configUrl.port}${apiConfig.getDataEndpoint}`);
    console.log('API /api/getData has been called automatically after server startup.');
  } catch (error) {
    console.error('Error while calling /api/getData:', error);
  }
});
