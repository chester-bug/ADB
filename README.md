## Member & Booking Modules: Oracle SQL/PLSQL Deliverable

This repository contains a single Oracle SQL script that creates a complete demo for a Member/Booking domain:

- Schema (tables and constraints)
- 1 sequence
- 2 indexes
- 2 views (used by the 2 management queries)
- 2 triggers (business rule and audit)
- 2 stored procedures (create booking, add payment)
- 2 report procedures (on-demand; both use nested cursors)
- 2 multi-table queries with SQL*Plus output formatting

### Files
- `sql/member_booking_oracle.sql` – Run this in an Oracle session (SQL*Plus, SQLcl, SQL Developer worksheet with `Server Output` enabled).

### Prerequisites
- Oracle Database 12c+ (tested syntax on 12c+ features; no identity columns used)
- `DBMS_OUTPUT` enabled where running reports

### How to Run
1. Open an Oracle SQL session and ensure `serveroutput` is on:
   ```sql
   SET SERVEROUTPUT ON SIZE UNLIMITED
   ```
2. Execute the script:
   ```sql
   @sql/member_booking_oracle.sql
   ```
3. The script creates all objects and prints nothing by default, but defines two formatted queries near the end. Rerun those query blocks as needed to view results.

### Using Procedures
Create a booking:
```sql
DECLARE
  v_booking_id NUMBER;
BEGIN
  -- assumes a member and room exist
  sp_create_booking(p_member_id => 1, p_room_id => 101, p_start_date => DATE '2025-09-10', p_end_date => DATE '2025-09-12', p_num_guests => 2, p_booking_id => v_booking_id);
  DBMS_OUTPUT.PUT_LINE('New booking id: ' || v_booking_id);
END;
/ 
```

Add a payment:
```sql
DECLARE
  v_payment_id NUMBER;
BEGIN
  sp_add_payment(p_booking_id => 1001, p_amount => 199.99, p_method => 'CARD', p_payment_id => v_payment_id);
  DBMS_OUTPUT.PUT_LINE('New payment id: ' || v_payment_id);
END;
/ 
```

### Running Reports (On-demand; nested cursors)
Member activity detail:
```sql
BEGIN
  report_member_activity(p_member_id => 1);
END;
/ 
```

Occupancy summary for a period:
```sql
BEGIN
  report_occupancy_summary(p_start_date => DATE '2025-09-01', p_end_date => DATE '2025-09-30');
END;
/ 
```

### Notes
- The overlap check for availability is implemented in the `sp_create_booking` procedure to avoid mutating table issues that arise when querying the same table inside a row-level trigger.
- Business rule validations (date range, capacity) are enforced both in the procedure and the trigger.
- `seq_app_id` is used to generate IDs across bookings, payments, and audit rows to satisfy the single-sequence requirement.
- Views `vw_member_ltv` and `vw_room_type_30d` power the two management queries with SQL*Plus output formatting included in the script.

