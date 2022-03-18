package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

const slotName = "pglogrepl_demo"

func main() {

	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGLOGREPL_DEMO_CONN_STRING"))
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("failed to retrieve Postgres system info (IDENTIFY_SYSTEM):", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "", pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.PhysicalReplication})
	if err != nil {
		log.Fatalln("failed to create temporary replication slot:", err)
	}
	log.Println("Created temporary replication slot:", slotName)

	sro := pglogrepl.StartReplicationOptions{Timeline: sysident.Timeline, Mode: pglogrepl.PhysicalReplication}
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, sro)
	if err != nil {
		log.Fatalln("failed to start replication:", err)
	}
	log.Println("Physical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	finishTimeout := time.Second * 15
	finishDeadline := time.Now().Add(finishTimeout)

	for {
		if time.Now().After(finishDeadline) {
			log.Println("Stopping replication since finish timeout expired", finishTimeout)
			break
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}
				log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData size", len(xld.WALData))

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}

	}
	copyDoneResult, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn)
	if err != nil {
		log.Fatalln("failed to end replicating:", err)
	}
	log.Println("Result of sending CopyDone:", copyDoneResult)

	sysident, err = pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("failed to retrieve Postgres system info (IDENTIFY_SYSTEM):", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, sro)
	if err != nil {
		log.Fatalln("failed to start replication:", err)
	}
	log.Println("Physical replication started on slot", slotName)

	ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
	msg, err := conn.ReceiveMessage(ctx)
	cancel()
	if err != nil {
		if pgconn.Timeout(err) {
			//continue
		}
		log.Fatalln("ReceiveMessage failed:", err)
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
			}
			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}
			log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData size", len(xld.WALData))

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	default:
		log.Printf("Received unexpected message: %#v\n", msg)
	}

}
