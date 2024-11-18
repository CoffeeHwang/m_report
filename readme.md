# 1. 개요 

## 코드의 목적
 - 엑셀파일 형식의 일일가공리포트를 생성하여 DB에 사전 등록된 업체의 메일 수신자에게 이메일 첨부 발송한다.
 - 2022년 기준 1000개 업체 동시 발송 성능 이상없음. 추후 부하 발생시 인스턴스 추가 필요함.

## 워크트리 
 - main.py : report, call_report 함수를 호출하는 함수 포함
 - yhs_common.py : 공통함수
 - yhs_mysql.py : mysql 연결 및 쿼리 기본실행
 - requirements.txt : 필요 패키지 목록

## 작동방식
   1. Google Cloud Scheduler 의 'report_scheduler' 가 매일 특정 시간에 수행된다. 
   2. 'report_scheduler' 가 PUB/SUB 'gcf_call_report_trigger' 토픽을 발행한다.    
   3. 'gcf_call_report_trigger' 토픽에 'call_report' Cloud Functions가 트리거 된다.
   4. 'call_report' 함수는 각 업체별 'gcf_report_trigger' 토픽을 발행한다. 
   5. 업체별 'gcf_report_trigger' 토픽에 'report' Cloud Functions가 각각 트리거 된다.
   6. 'report' 함수는 요청된 업체-날짜의 일일가공리포트를 생성하여 지정된 메일 수신자에게 발송한다.
---

# 2. 개발가이드

## python 개발환경 구축 (for Mac) 참고
> https://yhsbearing.atlassian.net/wiki/spaces/RNHM/pages/964689921/google+cloud+-+python+for+Mac

## 로컬환경 테스트 - 배포된 Google Cloud Functions 테스트 (PUS/SUB 발행후 트리거 방식)

### report 함수 테스트 (업체 지정 발송)
 - ex1) 업체 : id지정, 일자 : 특정일자 리포트, 메일수신자 직접지정 발송 : Hong.gildong@gmail.com    
    ``` 
    gcloud pubsub topics publish gcf_report_trigger --message '{"ent_id": "1", "report_date":"2023-03-16", "recv_email_addr":"Hong.gildong@gmail.com"}'
    ```  
 - ex2) 업체 : id지정, 일자 : 특정일자 리포트, **DB등록된 메일수신자** 모두에게 발송
    ```
    gcloud pubsub topics publish gcf_report_trigger --message '{"ent_id": "1", "report_date":"2023-03-16"}'
    ```
   - **DB등록된 메일수신자** 확인하는 방법  
     select cd ent_id, cd_name ent_name, value_str '수신이메일목록'  
       from ref_code where grp_cd = 'proc_report_recvs'  
      order by convert(cd, unsigned);   
     
### call_report 함수 테스트 (모든업체 순차 발송)
 - ex1) 모든 업체 대상으로 특정일자 발송
    ```        
    gcloud pubsub topics publish gcf_call_report_trigger --message '{"report_date":"2023-03-15"}'
    ```    
 - ex2) 모든 업체 대상으로 특정일자 발송(스케줄러에서 실행) + 특정 메일주소로만 발송
    ```
    gcloud pubsub topics publish gcf_call_report_trigger --message '{"report_date":"2023-03-15", "only_receive_email":"Hong.gildong@gmail.com"}'
    ```
 - ex3) 모든 업체 대상으로 전일자 리포트 발송(cloud 스케줄러에 사용하는 방법)
    ```
    gcloud pubsub topics publish gcf_call_report_trigger --message '{"report_date":"0000-00-00"}'
    ```

## 배포 방법

### Google Cloud Functions 코드배포 
배포하는 방법은 여러가지가 있으나, 콘솔을 통해 배포하는 방법을 소개한다. (추후 gcloud cli 를 이용하는 배포 방법을 통해 자동화를 고려해 볼 수 있다.)
1. 콘솔에서 Cloud Functions 메뉴로 이동한다.
2. 배포하고자 하는 함수 이름을 클릭한다.
3. 상단의 수정을 클릭한다.
4. 하단의 다음을 클릭한다.
5. 변경할 코드를 붙여넣는다.
6. 하단 배포를 클릭한다.

## 기타 사항 
- 환경변수 (.env) 파일은 배포된 Google Cloud Functions 소스탭에서 확인 가능
