---Creating Service Transfer Log Table to Record service transfer details and maintain an audit history
CREATE TABLE Service_Transfers (
    log_id INT IDENTITY PRIMARY KEY,
    service_id INT NOT NULL,
    src_aircraft_id INT NOT NULL,
    dest_aircraft_id INT NOT NULL,
    transfer_date DATETIME DEFAULT GETDATE()
);


---Procedure to implement the logic for transferring a service from one aircraft to another.
CREATE PROCEDURE sp_TransferService
    @srcAircraftID INT,
    @destAircraftID INT,
    @ServiceID INT
AS
BEGIN
    DECLARE @ServiceStatus VARCHAR(50)
    DECLARE @srcAircraftExists INT
    DECLARE @destAircraftExists INT
    DECLARE @SameAirline INT
    DECLARE @AssetID INT

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Ensure both source and destination aircraft exist, and the service is currently active
        SELECT @srcAircraftExists = COUNT(*) FROM Aircraft WHERE aircraft_id = @srcAircraftID;
        SELECT @destAircraftExists = COUNT(*) FROM Aircraft WHERE aircraft_id = @destAircraftID;

        IF @srcAircraftExists = 0 OR @destAircraftExists = 0
        BEGIN
            RAISERROR ('One or both aircraft do not exist.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
        END

        
        SELECT @ServiceStatus = status FROM Services WHERE service_id = @ServiceID;

        IF @ServiceStatus <> 'In Progress'
        BEGIN
            RAISERROR ('Service is not active.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
        END

        -- Check if source and destination aircraft must belong to the same airline
        SELECT @SameAirline = CASE WHEN A1.customer_id = A2.customer_id THEN 1 ELSE 0 END
        FROM Aircraft A1, Aircraft A2
        WHERE A1.aircraft_id = @srcAircraftID AND A2.aircraft_id = @destAircraftID;

        IF @SameAirline = 0
        BEGIN
            RAISERROR ('Aircraft must belong to the same airline.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
        END

        -- Ensure destination aircraft has compatible hardware
        SELECT @AssetID = asset_id FROM Assets WHERE aircraft_id = @destAircraftID;

        IF @AssetID IS NULL
        BEGIN
            RAISERROR ('Dest aircraft does not have a compatible asset', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
        END

        -- Update service table to reflect the new aircraft
        UPDATE Services
        SET aircraft_id = @destAircraftID
        WHERE service_id = @ServiceID;

        -- Log the transfer in the Service_Transfers table
        INSERT INTO Service_Transfers (service_id, src_aircraft_id, dest_aircraft_id, transfer_date)
        VALUES (@ServiceID, @srcAircraftID, @destAircraftID, GETDATE());

        COMMIT TRANSACTION;
        PRINT 'Service transfer successful';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;


---Identify the top five aircraft that received the highest number of service transfers in the past year.

SELECT TOP 5 aircraft_id, COUNT(*) AS transfer_count
FROM Service_Transfers
WHERE transfer_date >= DATEADD(YEAR, -1, GETDATE())
GROUP BY aircraft_id
ORDER BY transfer_count DESC;


---List all aircraft that had multiple service transfers within the same 30-day period.


SELECT DISTINCT a.aircraft_id
FROM Service_Transfers a
JOIN Service_Transfers b
    ON a.aircraft_id = b.aircraft_id 
    AND a.service_id <> b.service_id 
    AND ABS(DATEDIFF(DAY, a.transfer_date, b.transfer_date)) <= 30
ORDER BY a.aircraft_id;