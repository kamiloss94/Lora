
serialnumber = '80029805'
#serialnumber = input("Enter Serial Number ")
select_query = f"SELECT device_up.device_name, encode(data, 'hex')::text FROM public.device_up where device_name like '{serialnumber}' ORDER BY id DESC LIMIT 1"
      # column_names = ["id", "received_at", "dev_eui", "device_name", "application_id", "application_name",
       ##dev_addr", "confirmed_uplink", "tx_info"]
select_query_serial_numbers = f"SELECT distinct device_up.device_name FROM public.device_up"
column_names = ["device_name", "data"]
