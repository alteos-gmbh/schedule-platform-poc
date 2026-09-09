# Kịch bản demo

Bản dịch của [DEMO.md](DEMO.md). Bản tiếng Anh là bản gốc — nếu hai bản lệch nhau thì tin bản tiếng Anh.

Mười một hành vi cần bao phủ và mười một thứ cần cho xem. Mọi phát biểu về service cũ đều có file và số dòng đứng sau, nên nếu trong phòng có ai không đồng ý thì mở file ra là xong.

Chạy console (`npm run console`), mở <http://localhost:8787>, bấm **reset everything**.

---

## Bề mặt inbound đang được thay thế

| Route | Auth ở service cũ | Đã port |
| --- | --- | --- |
| `GET /v1/health` | không | có |
| `GET /v1/schedule` | **không có** | có |
| `POST /v1/schedule` | ACL `ScheduleCreateScheduledActions` | có, thay bằng IAM |
| `POST /v1/schedule/cancel` | ACL `ScheduleCancelScheduledActions` | có, thay bằng IAM |
| `POST /v1/schedule/activate` | **không có** | có |
| `POST /v2/schedule` | ACL create | có, thay bằng IAM |
| `DELETE /v2/schedule` | ACL cancel | có, thay bằng IAM |

Cộng hai cron job chạy trong process — `executeScheduledActions` và `resetLockedScheduledActions` (`createCron.ts`) — trở thành timer của Scheduler và reconciler.

## Mười một hành vi không nhìn thấy được từ danh sách route

| # | Hành vi | Nằm ở đâu trong service cũ |
| --- | --- | --- |
| 1 | Recurrence chống trôi: `beginAt + period × (counter+1)`, không phải `lastTrigger + period` | `processors/calculateNextTriggerDate.ts` |
| 2 | `endAt` so sánh ở mức **ngày**, `startOf('day') > endOf('day')` | `processors/executeScheduledActions.ts:206`, `:289` |
| 3 | …và phép so sánh đó phân giải theo **time zone của process**, không ở đâu pin nó lại | `convertToDateTime` trong `@alteos-gmbh/common` là `DateTime.fromISO` trần |
| 4 | Chọn V1 hay V2 bằng việc `messageTopicName` có mặt hay không, không phải bằng field version | `processors/executeScheduledActions.ts:130` |
| 5 | V1 tự tạo `authorizationData` của nó và chuyển `partnerId` sang `scopePartnerId` | nhánh V1 của `processors/executeScheduledActions.ts` |
| 6 | Topic kết thúc `.fifo` chạy tuần tự với `messageGroupId = context.policyId` | `processors/executeScheduledActions.ts:175` |
| 7 | Cancel match `context.command` **hoặc** `context.name`, và chỉ row `pending` | `processors/cancelScheduledActions.ts:31`, `:54` |
| 8 | `context.__waitForApproval` đưa row V1 vào `waitingExecutionApproval`; V2 bỏ qua nó | `api/createScheduledActionHandler.ts:19`, `:48-55` |
| 9 | `processingData` được ghi mỗi khi `period !== null` — nên period **vắng mặt** cũng được ghi | `api/createScheduledActionHandler.ts:61`, `api/createScheduledActionV2Handler.ts:54` |
| 10 | Sáu status, không phải bốn: có cả `preExecuted` và `waitingExecutionApproval` | `common/ScheduledActionStatus.ts` |
| 11 | Row kẹt ở `processing` chỉ được cứu bởi một cron job thứ hai sau `PROCESSING_TTL` | `processors/resetLockedScheduledActions.ts:19` |

`node src/test.mjs` kiểm 1, 2, 3, 5, 6, 8 và 9 mà không cần AWS. Số còn lại cho xem trực tiếp.

---

## 1 — Chuỗi định kỳ chạy

Tạo với **period `PT1M`**, bắn sau 60 giây — cả hai là giá trị mặc định của form, nên chỉ cần một cú bấm.

Nhìn: row đi `pending` → `processing` → `executed`, một row **mới** hiện ra ở `pending` với `counter` đã tăng, và một message rơi vào **Fired** mang theo `authorizationData` có `scopePartnerId` được set và `partnerId` là null — hành vi 5.

Nói: DynamoDB giữ cả chuỗi, Scheduler chỉ giữ một timer tại một thời điểm.

## 2 — Chuỗi không trôi

`counter` nhìn thấy được trên bảng; còn phép tính theo tháng thì không quan sát được trong một buổi demo, nên nó là test. `node src/test.mjs` cho thấy `2026-01-31 + P1M → 02-28`, rồi hop thứ hai quay lại đúng `03-31` chứ không phải `03-28`. Hành vi 1.

Cùng test đó cho thấy hành vi 3: đúng hai thời điểm ấy nhưng đọc ở UTC+7 lại cho đáp án khác, nên độ dài chuỗi của service cũ phụ thuộc `TZ` của container. Lambda của PoC khai `TZ=UTC`.

**Đo được ngoài kịch bản:** một chuỗi để chạy tự do 5,5 tiếng đạt `counter=169`, đi từ `04:03:49.100` tới `09:41:49.100` — giây và milliseconds y hệt sau 169 hop. Zero drift, và là bằng chứng mạnh hơn mọi thứ dựng được trong một buổi demo.

## 2b — Scheduler trễ ~30 giây, nên dưới một phút là không demo được

Đo 09.09.2026, `FlexibleTimeWindow` là `OFF` nên không có jitter do cấu hình. Một chuỗi `PT15S`:

| counter | triggerAt tính ra | bắn thật |
| --- | --- | --- |
| c0 | 03:42:52 | 03:43:32–03:43:37 |
| c1 | 03:43:07 | 03:44:10–03:44:16 |
| c2 | 03:43:22 | 03:44:54–03:45:00 |

Phép tính recurrence chính xác từng giây — 52, 07, 22, cách nhau đúng 15s. Nhưng khoảng cách giữa hai lần **bắn** là ~38s rồi ~44s. Đo từ mốc timer thực sự được đặt, độ trễ giao hàng của Scheduler là 25–37s, ba mẫu.

Nên nhịp thật ≈ `min_lead_seconds` + độ trễ Scheduler. Dưới một phút thì mọi occurrence kế đã nằm trong quá khứ lúc được tính, bị kẹp lên `now + min_lead_seconds`, và chuỗi chạy dồn để bắt kịp — nhìn như bùng nổ.

`PT1M` là mức nhỏ nhất còn sạch: occurrence kế nằm ~25s trong tương lai lúc fire xảy ra, nên không bị kẹp, và chuỗi chỉ trễ đều ~35s.

**Ba mẫu là ít** — ghi là "đo được", không phải giới hạn AWS công bố. Nhưng kết luận cho thiết kế thì đứng: platform này không giao được ở độ chính xác dưới phút. Nghiệp vụ thật không quan tâm; nhưng nếu có caller nào cần, phát hiện sau cutover là quá muộn. Cần tra `ALTEOS_CRON_TIME` của production để biết service cũ chặt hơn hay lỏng hơn.

## 3 — Chuỗi chết im lặng. Đây là business case.

Bật **legacy ordering** và **break chain write**. Tạo với period `PT1M`.

Khi nó bắn: row thành **`executed`**, không có row kế tiếp nào được tạo, danh sách timer rỗng đi, và **không có gì xuất hiện trong dead-letter queue**. Dấu vết duy nhất là một dòng log (`chainBrokenSilently`). Policy đó đã ngừng được tính phí và không có cảnh báo nào tồn tại.

Đây là `executeScheduledActions.ts`: `:182` đánh dấu row executed, `:229`/`:232` mới tạo occurrence kế tiếp, và `:236` bắt bất cứ gì throw ở giữa hai chỗ đó rồi chỉ log.

## 4 — Cùng lỗi đó, với thứ tự đã sửa

Tắt **legacy ordering**, để **break chain write** bật. Tạo với period `PT1M`.

Khi nó bắn: transaction fail, nên row đứng ở **`processing`** với `attempts` bò lên và `lastError` được ghi. Lambda retry hai lần — các lần retry cố ý nhận lại đúng row đó, vì nếu từ chối một row `processing` thì attempt thứ hai sẽ *thành công* và lỗi lại biến mất — và sau lần fail thứ ba thì một bản ghi hiện ra trong **Dead letters**, qua destination `aws_lambda_function_event_invoke_config`. Một phút sau reconciler đưa row về `pending` và tạo lại timer.

Không mất gì, và lỗi nhìn thấy được ở ba nơi thay vì không nơi nào.

Một hệ quả của tham số demo, không phải của thiết kế: `processing_ttl_minutes` và tick của reconciler đều là 1 phút ở đây, trong khi các lần retry async của Lambda giãn ra vài phút có backoff. Nên reconciler thường đưa row về `pending` *trong lúc* Lambda vẫn đang retry, và bản ghi dead-letter tới sau khi row đã trông như bình thường. Con số 15 phút trong thiết kế nằm hẳn ngoài cửa sổ retry của Lambda nên không đua. Nên nói ra, vì thứ tự trên màn hình không phải thứ tự production sinh ra.

Phơi nhiễm mà cách này để lại, và nó thuộc phần thảo luận thiết kế chứ không phải một dòng chú thích: một fire đã publish rồi *mới* fail sẽ publish lại ở mỗi lần retry. Đo ngày 08.09.2026 — đúng kịch bản này đặt **ba** bản sao lên target queue cho một schedule, và bản ghi dead-letter nói thẳng:

```
requestPayload  {"scheduleId":"8f0153e3-29f7-4b79-9481-bcb796c42578"}
condition       RetriesExhausted   approximateInvokeCount 3
errorMessage    simulated chain-write failure (POC breakNext)
```

`attempts` trên row là cùng con số đó. At-least-once cũng là thứ service cũ cho, nên đây không phải hồi quy — nhưng nó có nghĩa là mọi consumer của một schedule đã bắn đều phải idempotent, và chưa ai kiểm xem chúng có idempotent hay không. Xứng đáng một ticket.

## 5 — Publish lỗi

Chỉ bật **break publish**. Hình dạng giống mục 4, chỉ khác là lỗi đến từ lần ghi vào queue. Cho thấy cả hai đường lỗi đều rơi về cùng một chỗ.

## 6 — Cancel

Tạo một cái, rồi bấm **cancel policy** — `DELETE /v2/schedule?policyId=`. Row thành `cancelled` và timer biến khỏi danh sách Scheduler. Cả hai lần ghi, hoặc không lần nào.

**cancel command** chạy `POST /v1/schedule/cancel`, match theo `context.command` hoặc `context.name`. Hành vi 7.

## 7 — Cổng phê duyệt

Tạo với **`__waitForApproval`** được tick. Row hiện ra ở `waitingExecutionApproval` và cột timer hiển thị `—`: không có timer nào cả, nên nó không thể bắn. Bấm **activate** và nó thành `pending` kèm timer.

Hành vi 8 và 10. Đáng nói trong phòng: `activate` ở service cũ update theo `id IN (...)` **không có điều kiện status**, nên activate một row đã `cancelled` là hồi sinh nó. Đã tái hiện; gần như chắc chắn không phải ý định ban đầu.

## 8 — Reconciler sửa về phía row

Trên một row `pending`, bấm **drop timer**. Cột timer đổi thành `MISSING` — timer đã mất khỏi AWS trong khi ý định vẫn còn đó. Bấm **reconcile now** và nó quay lại.

Đây là thứ thay thế `resetLockedScheduledActions`, và nó làm nhiều hơn thế đáng kể.

## 9 — …và xoá thứ mà row không muốn

Trên một row `pending`, bấm **drop row**. Timer giờ là `ORPHAN`. Bấm **reconcile now** và nó bị xoá. Nếu nó bắn trước, dispatcher không publish gì và log `orphanFire`.

Gộp lại: mọi bất đồng đều phân giải về hướng của row trong DynamoDB.

## 10 — Idempotency

Bấm **activate** hai lần trên cùng một row. Lần `CreateSchedule` thứ hai gặp tên đã tồn tại, trả về `ConflictException`, và được đọc là bằng chứng lần đầu đã vào — không ghi đè, không bắn trùng. `ClientToken` không thay đổi gì và không được gửi. Đo ngày 07.09.2026.

## 11 — Câu hỏi trust policy trên terraform#608, và tại sao nó vẫn còn mở

```sh
cd infra
terraform apply -var expected_account_id=<account> -var scheduler_trust_form=two_statement
terraform apply -var expected_account_id=<account> -var scheduler_trust_form=source_arn_only
terraform apply -var expected_account_id=<account>   # account_only, mặc định
```

**Đừng trình bày cái này như đã chốt.** Những gì xảy ra ngày 08.09.2026 trong account 839810213476, eu-central-1, theo thứ tự:

| # | Cái gì chạy | Trust form | Kết quả |
| --- | --- | --- | --- |
| 1 | Terraform, apply lần đầu, role mới vài giây | two_statement | **fail** sau ~1m50s retry |
| 2 | Terraform, `ArnLike` nới thành `scheduler:*` | — | tạo trong 7s |
| 3 | Terraform, `ArnLike` trên ARN của group | — | tạo trong 1s |
| 4 | Terraform, chỉ ARN của group | — | tạo trong 2s |
| 5 | Terraform, chỉ `schedule/<group>/*` — đúng form đã fail ở #1 | source_arn_only | tạo trong 0s |
| 6 | Terraform, role tạo lại hoàn toàn (`-replace`) | source_arn_only | tạo trong 3s |
| 7 | Lambda, ba lần gọi liên tiếp, tên schedule mới hoàn toàn | two_statement | **fail** cả ba |
| 8 | Lambda, tên mới hoàn toàn | account_only | tạo được |
| 9 | Lambda, tên mới hoàn toàn | two_statement | tạo được |

Ba giả thuyết đã được thử và mỗi cái bị dòng kế tiếp giết: hình dạng condition (bị #5 giết), tuổi của role (bị #6 giết), và tên schedule là tên mới (bị #9 giết).

Nói được: lỗi là thật, nó không đều, và nó ngừng xảy ra sau khi trust policy được viết lại. Không nói được: form nào là đúng, hay là lỗi này không phải eventual consistency — comment trên #608 khẳng định nó không phải, và các dòng 1 và 7 so với 9 không đỡ cho khẳng định đó.

Chốt được việc này cần một trial harness sạch — role mới, tên schedule mới, N lần lặp mỗi form, đếm số fail — không phải thêm một lần thử lẻ nữa. Khoảng hai phút cho mỗi lần thử fail. Trước khi cái đó chạy, `account_only` là mặc định ở đây vì nó là form có ít lần fail được ghi nhận nhất, và `aws:SourceAccount` một mình vẫn giới hạn role trong Scheduler của account này.

## Một phát hiện nên mang vào bản implementation thật

Scheduler từ chối một biểu thức `at()` mang phần thập phân của giây:

```
Invalid Schedule Expression at(2026-09-08T03:54:26.439).
```

`suppressMilliseconds` của luxon chỉ bỏ chúng khi chúng đã bằng zero, nên nó không phải cách sửa — `toFormat("yyyy-MM-dd'T'HH:mm:ss")` mới là. Mọi `triggerAt` đến từ caller đều có milliseconds, nên cái này sẽ nổ ở lần gọi đầu tiên của bản implementation thật.

## Thứ tự FIFO, nếu có ai hỏi

Đặt **messageTopicName** thành `policy-obligations.fifo`. Các fire sẽ đi vào FIFO queue với `messageGroupId = policyId`, đó là cách thứ tự theo từng policy được giữ. Hành vi 6.

---

## Người xem không nên kết luận điều gì

- Đây không phải bản implementation. Nó là JavaScript thuần trong một account cá nhân; bản thật là TypeScript trong `serverless/schedule` trên hạ tầng từ `terraform#608`.
- Đường cancel rơi về full table scan với bất kỳ criterion nào khác `policyId`. Ổn ở số row của một PoC, không ổn ở production — DPT-10343 phải chọn một index hoặc một giới hạn.
- Chưa có gì ở đây được load-test, và quota schedule trên mỗi account của Scheduler chưa được đo đối chiếu với số policy đang sống. Đó là một câu hỏi mở thật, không phải một câu đã giải.
- Hình dạng trust policy chưa chốt — xem mục 11. Ba cách giải thích đã được thử và mỗi cái bị phép đo sau phủ định.
- Một fire được retry có thể publish nhiều lần. Xem mục 4.
