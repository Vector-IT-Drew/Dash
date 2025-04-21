
def format_phone(phone, default = 'N/A'):
	phone = str(phone)
	if len(phone) == 11:
		return  phone[1:-7] + '-' + phone[-7:-4] + '-' + phone[-4:]
	elif len(phone) ==10:
		return phone[0:-7] + '-' + phone[-7:-4] + '-' + phone[-4:]
	elif len(phone) < 1:
		return default
	return "___-___-____"

def format_dollar(val):
	try:
		if val < 0:
			return f"-${abs(val):,.2f}"  
		return f"${val:,.2f}" 
	except:
		return val