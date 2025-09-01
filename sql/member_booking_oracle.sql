-- Member & Booking Modules: Oracle SQL/PLSQL Deliverable
-- Includes: schema, 2 views, 2 formatted multi-table queries, 2 procedures, 2 triggers,
-- 2 report procedures with nested cursors, 1 sequence, 2 indexes, and exceptions.

set define off;
set serveroutput on size unlimited;

prompt Dropping existing objects (if any)...
DECLARE
  PROCEDURE drop_if_exists(p_sql VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE p_sql;
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
BEGIN
  drop_if_exists('DROP VIEW vw_member_ltv');
  drop_if_exists('DROP VIEW vw_room_type_30d');

  drop_if_exists('DROP TRIGGER trg_bookings_biu_validate');
  drop_if_exists('DROP TRIGGER trg_bookings_audit');

  drop_if_exists('DROP TABLE bookings_audit PURGE');
  drop_if_exists('DROP TABLE payments PURGE');
  drop_if_exists('DROP TABLE bookings PURGE');
  drop_if_exists('DROP TABLE rooms PURGE');
  drop_if_exists('DROP TABLE members PURGE');

  drop_if_exists('DROP SEQUENCE seq_app_id');
END;
/

prompt Creating base tables...
-- MEMBERS: Master data for customers/members
CREATE TABLE members (
  member_id        NUMBER         PRIMARY KEY,
  full_name        VARCHAR2(100)  NOT NULL,
  email            VARCHAR2(255)  NOT NULL UNIQUE,
  membership_level VARCHAR2(20)   DEFAULT 'STANDARD' CHECK (membership_level IN ('STANDARD','SILVER','GOLD','PLATINUM')),
  status           VARCHAR2(20)   DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','INACTIVE')),
  created_at       DATE           DEFAULT SYSDATE
);

-- ROOMS: Inventory of rooms/resources available for booking
CREATE TABLE rooms (
  room_id     NUMBER         PRIMARY KEY,
  room_number VARCHAR2(10)   NOT NULL UNIQUE,
  room_type   VARCHAR2(20)   NOT NULL CHECK (room_type IN ('STANDARD','DELUXE','SUITE')),
  capacity    NUMBER(2)      NOT NULL CHECK (capacity BETWEEN 1 AND 10),
  base_rate   NUMBER(10,2)   DEFAULT 0 NOT NULL,
  status      VARCHAR2(20)   DEFAULT 'AVAILABLE' CHECK (status IN ('AVAILABLE','MAINTENANCE','UNAVAILABLE'))
);

-- BOOKINGS: Transactions linking members to rooms with a date range
CREATE TABLE bookings (
  booking_id   NUMBER          PRIMARY KEY,
  member_id    NUMBER          NOT NULL REFERENCES members(member_id),
  room_id      NUMBER          NOT NULL REFERENCES rooms(room_id),
  start_date   DATE            NOT NULL,
  end_date     DATE            NOT NULL,
  num_guests   NUMBER(2)       DEFAULT 1 NOT NULL CHECK (num_guests BETWEEN 1 AND 10),
  status       VARCHAR2(20)    DEFAULT 'CONFIRMED' CHECK (status IN ('CONFIRMED','CHECKED_IN','CHECKED_OUT','CANCELLED')),
  cancel_reason VARCHAR2(200),
  created_at   DATE            DEFAULT SYSDATE
);

-- PAYMENTS: Monetization tied to bookings
CREATE TABLE payments (
  payment_id NUMBER         PRIMARY KEY,
  booking_id NUMBER         NOT NULL REFERENCES bookings(booking_id),
  amount     NUMBER(10,2)   NOT NULL CHECK (amount >= 0),
  paid_date  DATE           DEFAULT SYSDATE,
  method     VARCHAR2(20)   DEFAULT 'CARD' CHECK (method IN ('CARD','CASH','TRANSFER','VOUCHER')),
  note       VARCHAR2(200)
);

-- BOOKINGS_AUDIT: Row-change audit trail for bookings
CREATE TABLE bookings_audit (
  audit_id       NUMBER        PRIMARY KEY,
  booking_id     NUMBER        NOT NULL,
  action         VARCHAR2(10)  NOT NULL,
  action_ts      DATE          DEFAULT SYSDATE,
  action_user    VARCHAR2(128) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER'),
  old_status     VARCHAR2(20),
  new_status     VARCHAR2(20),
  old_start_date DATE,
  old_end_date   DATE,
  new_start_date DATE,
  new_end_date   DATE
);

prompt Creating one global sequence used across tables...
CREATE SEQUENCE seq_app_id START WITH 1 INCREMENT BY 1 NOCACHE;

prompt Creating indexes to support performance and validations...
-- Index to accelerate overlap checks and room availability lookups
CREATE INDEX idx_bookings_room_dates ON bookings (room_id, start_date, end_date);

-- Index to speed up payment lookups per booking
CREATE INDEX idx_payments_booking ON payments (booking_id);

prompt Creating views used by management queries...
-- View 1: Member Lifetime Value and activity
CREATE OR REPLACE VIEW vw_member_ltv AS
SELECT
  m.member_id,
  m.full_name AS member_name,
  m.membership_level,
  m.status AS member_status,
  COUNT(DISTINCT b.booking_id) AS total_bookings,
  NVL(SUM(CASE WHEN b.status <> 'CANCELLED' THEN p.amount ELSE 0 END), 0) AS total_revenue,
  MAX(b.start_date) AS last_booking_date
FROM members m
LEFT JOIN bookings b
  ON b.member_id = m.member_id
LEFT JOIN payments p
  ON p.booking_id = b.booking_id
GROUP BY m.member_id, m.full_name, m.membership_level, m.status;

-- View 2: 30-day forward occupancy and revenue by room type
CREATE OR REPLACE VIEW vw_room_type_30d AS
SELECT
  r.room_type,
  COUNT(DISTINCT r.room_id) AS num_rooms,
  -- Booked room-nights during the next 30 days (including in-stay overlaps)
  SUM(
    CASE
      WHEN b.status IN ('CONFIRMED','CHECKED_IN')
       AND b.end_date > TRUNC(SYSDATE)
       AND b.start_date < TRUNC(SYSDATE) + 30
      THEN GREATEST(LEAST(b.end_date, TRUNC(SYSDATE) + 30) - GREATEST(b.start_date, TRUNC(SYSDATE)), 0)
      ELSE 0
    END
  ) AS booked_room_nights,
  -- Revenue tied to bookings overlapping the next 30 days
  NVL(SUM(
    CASE
      WHEN b.end_date > TRUNC(SYSDATE)
       AND b.start_date < TRUNC(SYSDATE) + 30
      THEN p.amount ELSE 0
    END
  ), 0) AS booked_revenue
FROM rooms r
LEFT JOIN bookings b
  ON b.room_id = r.room_id
LEFT JOIN payments p
  ON p.booking_id = b.booking_id
GROUP BY r.room_type;

prompt Creating business rule trigger (dates, capacity, and id generation)...
CREATE OR REPLACE TRIGGER trg_bookings_biu_validate
BEFORE INSERT OR UPDATE OF start_date, end_date, num_guests, room_id ON bookings
FOR EACH ROW
DECLARE
  l_capacity rooms.capacity%TYPE;
BEGIN
  -- Auto-assign booking_id from the global sequence if not provided
  IF INSERTING AND :NEW.booking_id IS NULL THEN
    SELECT seq_app_id.NEXTVAL INTO :NEW.booking_id FROM dual;
  END IF;

  -- Validate date range
  IF :NEW.end_date <= :NEW.start_date THEN
    RAISE_APPLICATION_ERROR(-20010, 'End date must be after start date.');
  END IF;

  -- Validate room capacity against num_guests
  SELECT capacity INTO l_capacity FROM rooms WHERE room_id = :NEW.room_id;
  IF :NEW.num_guests > l_capacity THEN
    RAISE_APPLICATION_ERROR(-20011, 'Guest count exceeds room capacity.');
  END IF;
END;
/

prompt Creating audit trigger for bookings...
CREATE OR REPLACE TRIGGER trg_bookings_audit
AFTER INSERT OR UPDATE OR DELETE ON bookings
FOR EACH ROW
DECLARE
  l_action VARCHAR2(10);
  l_audit_id NUMBER;
BEGIN
  IF INSERTING THEN
    l_action := 'INSERT';
  ELSIF UPDATING THEN
    l_action := 'UPDATE';
  ELSE
    l_action := 'DELETE';
  END IF;

  SELECT seq_app_id.NEXTVAL INTO l_audit_id FROM dual;

  INSERT INTO bookings_audit (
    audit_id, booking_id, action, action_ts, action_user,
    old_status, new_status,
    old_start_date, old_end_date,
    new_start_date, new_end_date
  ) VALUES (
    l_audit_id,
    CASE WHEN INSERTING THEN :NEW.booking_id ELSE :OLD.booking_id END,
    l_action,
    SYSDATE,
    SYS_CONTEXT('USERENV','SESSION_USER'),
    CASE WHEN INSERTING THEN NULL ELSE :OLD.status END,
    CASE WHEN DELETING THEN NULL ELSE :NEW.status END,
    CASE WHEN INSERTING THEN NULL ELSE :OLD.start_date END,
    CASE WHEN INSERTING THEN NULL ELSE :OLD.end_date END,
    CASE WHEN DELETING THEN NULL ELSE :NEW.start_date END,
    CASE WHEN DELETING THEN NULL ELSE :NEW.end_date END
  );
END;
/

prompt Creating stored procedures with validations and exceptions...
-- Procedure 1: Create a booking with business validations and overlap check
CREATE OR REPLACE PROCEDURE sp_create_booking (
  p_member_id   IN  members.member_id%TYPE,
  p_room_id     IN  rooms.room_id%TYPE,
  p_start_date  IN  DATE,
  p_end_date    IN  DATE,
  p_num_guests  IN  bookings.num_guests%TYPE,
  p_booking_id  OUT bookings.booking_id%TYPE
) AS
  l_member_status members.status%TYPE;
  l_capacity      rooms.capacity%TYPE;
  l_overlap_count NUMBER;
BEGIN
  -- Validate member exists and active
  SELECT status INTO l_member_status FROM members WHERE member_id = p_member_id;
  IF l_member_status <> 'ACTIVE' THEN
    RAISE_APPLICATION_ERROR(-20001, 'Member is not ACTIVE.');
  END IF;

  -- Validate room exists and capacity
  SELECT capacity INTO l_capacity FROM rooms WHERE room_id = p_room_id;
  IF p_num_guests > l_capacity THEN
    RAISE_APPLICATION_ERROR(-20002, 'Guest count exceeds room capacity.');
  END IF;

  -- Validate date range
  IF p_end_date <= p_start_date THEN
    RAISE_APPLICATION_ERROR(-20003, 'End date must be after start date.');
  END IF;

  -- Overlap check: room must be free for the requested range
  SELECT COUNT(*) INTO l_overlap_count
  FROM bookings b
  WHERE b.room_id = p_room_id
    AND b.status IN ('CONFIRMED','CHECKED_IN')
    AND b.end_date > p_start_date
    AND b.start_date < p_end_date;

  IF l_overlap_count > 0 THEN
    RAISE_APPLICATION_ERROR(-20004, 'Room is not available for the requested period.');
  END IF;

  -- Insert booking
  p_booking_id := seq_app_id.NEXTVAL;
  INSERT INTO bookings (
    booking_id, member_id, room_id, start_date, end_date, num_guests, status, created_at
  ) VALUES (
    p_booking_id, p_member_id, p_room_id, p_start_date, p_end_date, p_num_guests, 'CONFIRMED', SYSDATE
  );
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20005, 'Member or Room not found.');
  WHEN OTHERS THEN
    RAISE; -- let caller see details for unexpected errors
END;
/

-- Procedure 2: Add a payment for a booking, using PRAGMA EXCEPTION_INIT
CREATE OR REPLACE PROCEDURE sp_add_payment (
  p_booking_id IN payments.booking_id%TYPE,
  p_amount     IN payments.amount%TYPE,
  p_method     IN payments.method%TYPE,
  p_payment_id OUT payments.payment_id%TYPE
) AS
  ex_fk_violation EXCEPTION;
  PRAGMA EXCEPTION_INIT(ex_fk_violation, -2291); -- ORA-02291: integrity constraint violated - parent key not found
  l_status bookings.status%TYPE;
BEGIN
  IF p_amount < 0 THEN
    RAISE_APPLICATION_ERROR(-20006, 'Amount cannot be negative.');
  END IF;

  -- Ensure booking exists and not cancelled
  SELECT status INTO l_status FROM bookings WHERE booking_id = p_booking_id;
  IF l_status = 'CANCELLED' THEN
    RAISE_APPLICATION_ERROR(-20007, 'Cannot add payment to a CANCELLED booking.');
  END IF;

  p_payment_id := seq_app_id.NEXTVAL;
  INSERT INTO payments (payment_id, booking_id, amount, paid_date, method)
  VALUES (p_payment_id, p_booking_id, p_amount, SYSDATE, NVL(p_method, 'CARD'));
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20008, 'Booking does not exist.');
  WHEN ex_fk_violation THEN
    RAISE_APPLICATION_ERROR(-20009, 'Invalid booking reference for payment.');
  WHEN OTHERS THEN
    RAISE;
END;
/

prompt Creating report procedures (nested cursors)...
-- Report 1 (On-demand Detail): Member activity with nested cursors (bookings -> payments)
CREATE OR REPLACE PROCEDURE report_member_activity (
  p_member_id IN members.member_id%TYPE
) AS
  CURSOR c_bookings IS
    SELECT b.booking_id, b.start_date, b.end_date, b.status, b.room_id
    FROM bookings b
    WHERE b.member_id = p_member_id
    ORDER BY b.start_date DESC;

  CURSOR c_payments (cp_booking_id bookings.booking_id%TYPE) IS
    SELECT payment_id, amount, paid_date, method
    FROM payments
    WHERE booking_id = cp_booking_id
    ORDER BY paid_date;

  l_member_name members.full_name%TYPE;
  l_total_bookings NUMBER := 0;
  l_total_paid     NUMBER := 0;
BEGIN
  SELECT full_name INTO l_member_name FROM members WHERE member_id = p_member_id;

  DBMS_OUTPUT.PUT_LINE('Member Activity Report for ' || l_member_name || ' (ID ' || p_member_id || ')');
  DBMS_OUTPUT.PUT_LINE('----------------------------------------------------------------------');

  FOR r_b IN c_bookings LOOP
    l_total_bookings := l_total_bookings + 1;
    DBMS_OUTPUT.PUT_LINE('Booking ' || r_b.booking_id || ' Room ' || r_b.room_id ||
      ' [' || TO_CHAR(r_b.start_date, 'YYYY-MM-DD') || ' to ' || TO_CHAR(r_b.end_date, 'YYYY-MM-DD') || '] Status: ' || r_b.status);
    -- Nested cursor: payments per booking
    FOR r_p IN c_payments(r_b.booking_id) LOOP
      l_total_paid := l_total_paid + NVL(r_p.amount,0);
      DBMS_OUTPUT.PUT_LINE('  Payment ' || r_p.payment_id || ': ' || TO_CHAR(r_p.amount, 'FM999,999,990.00') ||
        ' on ' || TO_CHAR(r_p.paid_date, 'YYYY-MM-DD') || ' via ' || r_p.method);
    END LOOP;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('----------------------------------------------------------------------');
  DBMS_OUTPUT.PUT_LINE('Total bookings: ' || l_total_bookings);
  DBMS_OUTPUT.PUT_LINE('Total paid:     ' || TO_CHAR(l_total_paid, 'FM999,999,990.00'));
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    DBMS_OUTPUT.PUT_LINE('No such member: ' || p_member_id);
END;
/

-- Report 2 (Summary On-demand with Nested Cursors): Occupancy summary by room type and room
CREATE OR REPLACE PROCEDURE report_occupancy_summary (
  p_start_date IN DATE,
  p_end_date   IN DATE
) AS
  CURSOR c_types IS
    SELECT DISTINCT room_type FROM rooms ORDER BY room_type;

  CURSOR c_rooms (cp_room_type rooms.room_type%TYPE) IS
    SELECT room_id, room_number FROM rooms WHERE room_type = cp_room_type ORDER BY room_number;

  CURSOR c_room_bookings (
    cp_room_id rooms.room_id%TYPE,
    cp_start   DATE,
    cp_end     DATE
  ) IS
    SELECT booking_id, start_date, end_date, status
    FROM bookings
    WHERE room_id = cp_room_id
      AND end_date > cp_start
      AND start_date < cp_end
    ORDER BY start_date;

  l_type_nights NUMBER;
  l_room_nights NUMBER;
  l_type_total_nights NUMBER;
  l_period_days NUMBER := GREATEST(TRUNC(p_end_date) - TRUNC(p_start_date), 0);
  l_type_room_count NUMBER;
BEGIN
  IF p_end_date <= p_start_date THEN
    RAISE_APPLICATION_ERROR(-20020, 'Report period end_date must be after start_date.');
  END IF;

  DBMS_OUTPUT.PUT_LINE('Occupancy Summary ' || TO_CHAR(p_start_date, 'YYYY-MM-DD') || ' to ' || TO_CHAR(p_end_date, 'YYYY-MM-DD'));
  DBMS_OUTPUT.PUT_LINE('================================================================================');

  FOR r_type IN c_types LOOP
    l_type_total_nights := 0;
    DBMS_OUTPUT.PUT_LINE('Room Type: ' || r_type.room_type);
    -- Precompute number of rooms of this type (avoid scalar subquery in PL/SQL expression)
    SELECT COUNT(*) INTO l_type_room_count FROM rooms WHERE room_type = r_type.room_type;

    FOR r_room IN c_rooms(r_type.room_type) LOOP
      l_room_nights := 0;
      FOR r_b IN c_room_bookings(r_room.room_id, p_start_date, p_end_date) LOOP
        l_room_nights := l_room_nights + GREATEST(LEAST(r_b.end_date, p_end_date) - GREATEST(r_b.start_date, p_start_date), 0);
      END LOOP;
      l_type_total_nights := l_type_total_nights + l_room_nights;
      DBMS_OUTPUT.PUT_LINE('  Room ' || r_room.room_number || ' booked nights: ' || l_room_nights);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('  Type total booked nights: ' || l_type_total_nights);
    DBMS_OUTPUT.PUT_LINE('  Type occupancy %: ' ||
      TO_CHAR(CASE WHEN l_period_days * l_type_room_count = 0 THEN 0
                   ELSE (l_type_total_nights / (l_period_days * l_type_room_count)) * 100 END,
              'FM990.00') || '%');
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------------------------------------');
  END LOOP;
END;
/

prompt Two formatted multi-table queries (using the views)...
-- Query 1 (Strategic): Top members by lifetime revenue and activity
-- Formatting (SQL*Plus)
SET PAGESIZE 100
SET LINESIZE 200
COLUMN member_name FORMAT A28
COLUMN membership_level FORMAT A10
COLUMN member_status FORMAT A8
COLUMN total_bookings HEADING 'BOOKINGS' FORMAT 9990
COLUMN total_revenue HEADING 'TOTAL_REVENUE' FORMAT 999,999,990.00
COLUMN last_booking_date HEADING 'LAST_BOOKING' FORMAT A12

-- Actual query
SELECT
  l.member_id,
  l.member_name,
  l.membership_level,
  l.member_status,
  l.total_bookings,
  l.total_revenue,
  TO_CHAR(l.last_booking_date, 'YYYY-MM-DD') AS last_booking_date
FROM vw_member_ltv l
ORDER BY l.total_revenue DESC, l.total_bookings DESC
FETCH FIRST 10 ROWS ONLY;

-- Query 2 (Tactical/Operational): 30-day forward occupancy and revenue by room type
-- Formatting (SQL*Plus)
SET PAGESIZE 100
SET LINESIZE 200
COLUMN room_type FORMAT A10
COLUMN num_rooms HEADING 'ROOMS' FORMAT 9990
COLUMN booked_room_nights HEADING 'BOOKED_NIGHTS' FORMAT 999,999,990
COLUMN occupancy_pct HEADING 'OCCUPANCY_%' FORMAT 990.00
COLUMN booked_revenue HEADING 'BOOKED_REVENUE' FORMAT 999,999,990.00

-- Actual query
SELECT
  v.room_type,
  v.num_rooms,
  v.booked_room_nights,
  ROUND((v.booked_room_nights / (v.num_rooms * 30)) * 100, 2) AS occupancy_pct,
  v.booked_revenue
FROM vw_room_type_30d v
ORDER BY v.room_type;

prompt Done.

