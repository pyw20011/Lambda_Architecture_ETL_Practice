# Lambda_Architecture_ETL_Practice

황세규, 『자바와 파이썬으로 만드는 빅데이터 시스템』, 제이펍(2023)의 예제 코드를 따라 작성해보며 실습한 코드입니다.<br>
출판사 링크 : https://jpub.tistory.com/1414 (예제 코드 원본 링크도 함께 있음)   

<img width="1100" alt="프로세스 절차" src="https://github.com/user-attachments/assets/14b95c35-5892-4508-94e7-ed41ce57d4eb" />
batch 처리와 streaming 처리를 동시에 진행하는 람다 아키텍처 기반의 ETL 프로세스이며,     
다음 절차들로 구성되어 있습니다.<br><br>


1. FRED(연방 준비 은행 경제 데이터)로부터 데이터를 추출하여 HDFS에 저장
2. Apache Kafka를 통해 HDFS에서 Spark로 데이터 옮기기
3. Apache Spark를 이용하여 데이터를 처리하고, MySQL(배치)/MongoDB(스트리밍) 데이터마트로 데이터 적재
4. 각 데이터베이스에서 용도에 맞게 데이터를 추출하여 머신러닝을 위한 데이터셋으로 정제
