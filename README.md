# Lambda_Architecture_ETL_Practice

황세규, 『자바와 파이썬으로 만드는 빅데이터 시스템』, 제이펍(2023)의 예제 코드를 그대로 따라 작성해보며 실습한 코드입니다.
출판사 링크 : https://jpub.tistory.com/1414 (예제 코드 원본 링크도 함께 있음)

batch 처리와 streaming 처리를 동시에 진행하는 람다 아키텍처 기반의 ETL 프로세스이며,

1. FRED(연방 준비 은행 경제 데이터)로부터 데이터를 추출하여 HDFS에 저장
2. 아파치 카프카를 통해 HDFS에서 Spark로 데이터 옮기기
3. Spark SQL을 이용하여 MySQL 데이터마트로 데이터 적재
4. Spark Structured Streaming을 이용하여 MongoDB 데이터마트로 데이터 적재
5. 각 데이터베이스에서 데이터를 추출하여 활용
의 절차로 이루어집니다.
