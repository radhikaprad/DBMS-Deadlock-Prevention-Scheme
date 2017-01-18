import sys
import os
import collections
import pdb

filepath="/Users/radhikamanivannan/Documents/DBMS2/IP1.txt"
temp=[]
tempwithoutdecl=[]
ttable=[]
ltable=[]


states=["active","blocked","aborted","commit"]
lockstates=["read","write"]

Transactiontable = collections.namedtuple("Transactiontable" ,["Trans_id","Timestamp", "TState", "Listofitemwaiting"])
LockTable=collections.namedtuple("LockTable",["itemname","lockstate","Tid","othertransreadlock","waitingtrans"])
LockTable.othertransreadlock=[]
LockTable.waitingtrans=[]
listforblocks=[]
listforaborts=[]


with open(filepath,'r') as f:
	data=f.read()

line=data.split("\n")
for i in line:
	temp.append(i.split("\r")[0])
for i in temp:
	tempwithoutdecl.append(i.split(";")[0])


for i in tempwithoutdecl:
	if "b" in i:
		print (i)
		templine_b=i.split("b")[1]
		timestamp=templine_b
		ttable.append(Transactiontable(templine_b,timestamp,states[0],""))
		print ("T{0} appended to Transaction table with state active") .format(templine_b)
		print(ttable)
	elif "r" in i:
		print (i)
		itemfound=False
		templine=i.split("r")[1]
		id_ofitem=templine.split(" ")[0]
		if len(id_ofitem)>1:
			id_ofitem=id_ofitem.split("(")[0]

		tempitemname=i.split("(")[1]
		itemname=tempitemname.split(")")[0]

		for indx in range(0,len(ltable)):
			if ltable[indx].itemname==itemname:
				itemfound=True
				intable_itemname=ltable[indx].itemname
				print("\n")
				print(intable_itemname)

				if id_ofitem!=ltable[indx].Tid:
					if ltable[indx].lockstate=="read":
						othertransarray = ltable[indx].othertransreadlock
						othertransarray.append(id_ofitem)
						ltable[indx] = ltable[indx]._replace(othertransreadlock=othertransarray)
						
					elif ltable[indx].lockstate=="write":
						Tid=ltable[indx].Tid
						for tt in ttable:
							if tt.Trans_id==Tid:
								Ts_write=tt.Timestamp

						for tat in range(0,len(ttable)):
							if int(id_ofitem)==int(ttable[tat].Trans_id):
								print("line 75")
								print(ttable[tat])
								
								Timestamp_ofread=ttable[tat].Timestamp
								if int(Ts_write)<int(Timestamp_ofread):
									ttable[tat]=ttable[tat]._replace(TState=states[1])
									waitarray = []
									for waittransval in ltable[indx].waitingtrans:
										waitarray.append(waittransval)
									waitarray.append(id_ofitem)
									print('{0} in l table and {1} in file') .format(ltable[indx].itemname,itemname)
									ltable[indx]=ltable[indx]._replace(waitingtrans=waitarray)
									print(ttable)
									print(ltable)
									print 'T{0} is blocked by T{1} for dataitem {2}' .format(id_ofitem,Tid,itemname)
									listforblocks.append(i)
									print(listforblocks)

								else:
									for ltt in range(0,len(ttable)):
										if Tid==ttable[ltt].Trans_id:
											ttable[ltt]=ttable[ltt]._replace(TState=states[2])
											
											print 'T{0} is aborted by T{1} for dataitem {2}' .format(Tid,id_ofitem,itemname)
											listforaborts.append(i)
											print(listforaborts)


		if itemfound==False:
			print '{0} is entered in locktable with {1}' .format(itemname,lockstates[0])
			ltable.append(LockTable(itemname,lockstates[0],id_ofitem, [],[]))
		
	
	elif "w" in i:
		print(i)
		templine=i.split("w")[1]
		idofitem=templine.split(" ")[0]
		tempitemname=i.split("(")[1]
		itemname=tempitemname.split(")")[0]
		tempfound=False
		temp=""


		if len(idofitem)>1:
			idofitem=idofitem.split("(")[0]
		
		for ind in range(0,len(ltable)):		
			if ltable[ind].itemname==itemname:
				
				"""for st in range(0,len(ttable)):
																	if ttable[st].waitingtrans==idofitem:
																		blocked=ttable[st].waitingtrans
																		Tstate=ttable[st].TState"""

				
				if ltable[ind].lockstate=="read":
					
					if int(ltable[ind].Tid)==int(idofitem):
						print '{0} is entered in locktable with {1}' .format(itemname,lockstates[1])
						tempfound=True
						temp=itemname
						print(temp)
					else:
						
						for trt in ttable:
							if trt.Trans_id==ltable[ind].Tid:
								idintable_TS=trt.Timestamp

						for trt in range(0,len(ttable)):

							if ttable[trt].Trans_id==idofitem:
								idofitem_Tid=ttable[trt].Trans_id
								idofitem_TS=ttable[trt].Timestamp
								
								if int(idintable_TS)<int(idofitem_TS):
									ttable[trt] = ttable[trt]._replace(TState=states[1])
									print '{0} is blocked by {1}' .format(idofitem_Tid,idofitem)
									listforblocks.append(i)
									print(listforblocks)
									print(ttable[trt])
								else:
									ttable[trt] = ttable[trt]._replace(TState=states[2])
									print '{0} is aborted by {1}' .format(idofitem_Tid,ltable[ind].Tid)
									print(ttable[trt])
									listforaborts.append(i)
									print(listforaborts)
				


				elif ltable[ind].lockstate=="write":
					Tid=ltable[ind].Tid
					for tab in range(0,len(ttable)):

						if Tid==ttable[tab].Trans_id:
							Timestamp_write=ttable[tab].Timestamp
						elif idofitem==ttable[tab].Trans_id:
							Timestamp_write_item=ttable[tab].Timestamp
					if int(Timestamp_write)<int(Timestamp_write_item):
						for tb in range(0,len(ttable)):	
							if idofitem==ttable[tb].Trans_id:
								ttable[tb]=ttable[tb]._replace(TState=states[1])
								print("inside write")
								print(ttable[tb])
							elif Tid==ttable[tb].Trans_id:
								ttable[tb]=ttable[tb]._replace(Listofitemwaiting=idofitem)
								print("inside write")
								print(ttable[tb])
					else:
						for tb in range(0,len(ttable)):	
							
							if Tid==ttable[tb].Trans_id:
								ttable[tb]=ttable[tb]._replace(TState=states[2])
								print(ttable[tb])

		if tempfound==True:
			for i in range(0,len(ltable)):
				if ltable[i].itemname==itemname:
					ltable[i]=ltable[i]._replace(lockstate=lockstates[1])
			print(ltable)

	elif "c" in i:
		print (i)
		templine_b=i.split("c")[1]
		print(templine_b)
		timestamp=templine_b
		it=0
		for tabs in range(0,len(ttable)):
			if templine_b==ttable[tabs].Trans_id:
				ttable[tabs]=ttable[tabs]._replace(TState=states[3])
				print(ttable[tabs])
				waittrans = []
				for ltindx in range (0,len(ltable)):
					if int(templine_b)==int(ltable[ltindx].Tid):
						print('line 207 ltable array {0} {1}') .format(ltable[ltindx], ltable[ltindx][4])	
						for waittransval in ltable[ltindx][4]:
							waittrans.append(waittransval)
				print(listforblocks)
				for ltindx in range (0,len(ltable)):
					ltable[:] = (value for value in ltable if value.Tid != templine_b)

				for i in range(0,len(listforblocks)):
					print(listforblocks[i])
					if "r" in listforblocks[i]:
						templine=listforblocks[i].split("r")[1]
						id_ofitem=templine.split(" ")[0]
						if len(id_ofitem)>1:
							id_ofitem=id_ofitem.split("(")[0]
						tempitemname=listforblocks[i].split("(")[1]
						itemname=tempitemname.split(")")[0]
						if waittrans[it]==id_ofitem:
							ltable.append(LockTable(itemname,lockstates[0],id_ofitem,"",""))
						print(ltable)



					elif "w" in listforblocks[i]:
						templine=listforblocks[i].split("w")[1]
						id_ofitem=templine.split(" ")[0]
						if len(id_ofitem)>1:
							id_ofitem=id_ofitem.split("(")[0]
						tempitemname=listforblocks[i].split("(")[1]
						itemname=tempitemname.split(")")[0]
						if waittrans[it]==id_ofitem:
							ltable.append(LockTable(itemname,lockstates[0],id_ofitem,[],[]))
						print(ltable)







print(ttable)
print("\n")
print("_____________Locktable_____________ ") 
print("\n")
print("{0}").format(ltable)

