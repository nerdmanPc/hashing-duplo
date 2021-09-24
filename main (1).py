from struct import Struct
from enum import Enum
from typing import Optional, Tuple
import sys, os, math

FILE_PATH = "data.bin"
MAXNUMREGS = 11

#Função de hash h1
def h1(key:int, len:int):
    hash_1 = key % len
    return hash_1

#Função de hash h2
def h2(key:int, len:int):
    value = math.floor(key / len)
    hash_2 = max(value % len, 1)
    return hash_2

#Função de hashing duplo para lidar com colisões
def double_hashing(key:int):
	database = DataBase(FILE_PATH)
	position_found = False
	index = 1
	
	while index <= MAXNUMREGS:
		new_position = (h1(key) + index * h2(key)) % MAXNUMREGS

		if database.entry_by_index(index) == None:
			position_found = True
			break
		else:
			index += 1
	return position_found, new_position

class EntryStatus(Enum):
	EMPTY = 0
	FULL = 1
	REMOVED = 2

class Entry:
	format = Struct('> B L 20s B')

	def __init__(self, status: EntryStatus, key:int, name:str, age:int) -> None: 
		self.status = status
		self.key = key
		self.name = name
		self.age = age

	@classmethod
	def size(cls) -> int:
		return cls.format.size

	def has_data(self) -> bool:
		return self.status == EntryStatus.FULL

	def is_key(self, key:int) -> bool:
		if self.has_data():
			return self.key == key
		else:
			return False
	
	@classmethod
	def from_status(cls, status:EntryStatus):
		return cls(status, 0, '', 0)

	@classmethod
	def from_fields(cls, key:int, name:str, age:int):
		return cls(EntryStatus.FULL, key, name, age)

	@classmethod
	def from_bytes(cls, data:bytes): # -> Entry
		(status, key, name, age) = cls.format.unpack(data)
		#print(f'status: [{status}] key: [{key}] name: [{name}] age: [{age}]')
		status = EntryStatus(status)
		return cls(status, key, name, age)

	def into_bytes(self) -> bytes:
		if self.status == EntryStatus.FULL:
			return self.format.pack(self.status.value, self.key, bytes(self.name, 'utf-8'), self.age)
		else:
			return self.format.pack(self.status.value, 0, b'', 0)

	def __str__(self):
		if self.status == EntryStatus.FULL:
			return f'{self.key} {self.name} {self.age}'
		elif self.status == EntryStatus.EMPTY:
			return 'vazio'
		else:
			return '*'


class OpStatus(Enum): 
	OK = 0 
	ERR_KEY_EXISTS = -1
	ERR_OUT_OF_SPACE = -2
	ERR_KEY_NOT_FOUND = -3

class DataBase:
	header_format = Struct('>L')

	def __init__(self, file_path:str):
		self.path = file_path
		try:
			with open(file_path, 'xb') as file:
				self.length = MAXNUMREGS
				file.write( self.header_format.pack(self.length) )
				empty_entry = Entry.from_status(EntryStatus.EMPTY).into_bytes()
				file.write(empty_entry * self.length)
		except FileExistsError:
			with open(file_path, "rb") as file:
				header = file.read(self.header_size())
				_tuple = self.header_format.unpack(header)
				self.length = _tuple[0]

	def __iter__(self):
		self.it_index = -1
		self.it_file = open(self.path, 'rb')
		self.it_file.seek(self.header_size(), 0)
		return self

	def __next__(self):
		self.it_index += 1
		if self.it_index >= self.length:
			self.it_file.close()
			raise StopIteration
		data = self.it_file.read(Entry.size())
		entry = Entry.from_bytes(data)
		return entry

	def __str__(self):
		result = []
		for entry in self:
			result.append( str(entry) )
		return '\n'.join(result)

	@classmethod
	def header_size(cls) -> int:
		return cls.header_format.size

	@classmethod
	def index_to_ptr(cls, index:int) -> int:
		entry_size = Entry.size()
		header_size = cls.header_size()
		return header_size + index * entry_size

	def set_length(self, length:int) -> None:
		with open(self.path, 'r+b') as file:
			file.seek(0, 0)
			file.write(self.header_format.pack(length))
			self.length = length

	def index_by_key(self, key:int) -> Optional[int]:
		for entry in self:
			if entry.is_key(key):
				return self.it_index
		return None

	#Função de hashing duplo para lidar com colisões
	def double_hashing(self, key:int):
		#database = DataBase(FILE_PATH)
		position_found = False
		index = 1

		while index <= self.length:
			new_position = (h1(key, self.length) + index * h2(key, self.length)) % self.length

			if self.entry_by_index(index) is None:
				position_found = True
				break
			else:
				index += 1
		return position_found, new_position

	def delete_by_index(self, to_delete:int) -> None:
		with open(self.path, 'r+b') as file:
			entry_ptr = self.index_to_ptr(to_delete)
			file.seek(entry_ptr)
			bytes_to_write = Entry.from_status(EntryStatus.REMOVED).into_bytes()
			file.write(bytes_to_write)

	def entry_by_index(self, index:int) -> Optional[Entry]:
		with open(self.path, 'rb') as file:
			if index < self.length:
				file.seek(self.header_size() + index * Entry.size(), 0)
				data = file.read(Entry.size())
				entry = Entry.from_bytes(data)
				return entry
			else:
				return None

	def add_entry(self, key:int, name:str, age:int) -> OpStatus:
		position_found = False
		position = h1(key)
		print('position', position)
		search_result = self.index_by_key(position)
		print(search_result)
		#Se a posição do registro não é vazia
		if search_result is not None:
			#return OpStatus.ERR_KEY_EXISTS
			print('Teve colisão!')
			while not position_found:
				position_found, new_position = double_hashing(key)
				if position_found:
					key = new_position
		with open(self.path, 'r+b') as file:
			end_pointer = self.index_to_ptr(self.length)
			file.seek(end_pointer)
			file.write( Entry(key, name, age).into_bytes() )
			self.set_length(self.length + 1)
		return OpStatus.OK


	def delete_by_key(self, key:int) -> OpStatus:
		index = self.index_by_key(key)
		if index is None:
			return OpStatus.ERR_KEY_NOT_FOUND
		self.delete_by_index(index)
		return OpStatus.OK

    #Retorna entrada através de uma key
	def entry_by_key(self, key:int) -> Optional[Entry]:
		with open(self.path, 'rb') as file:
			for i in range(self.length):
				file.seek(self.header_size() + i * Entry.size(), 0)
				data = file.read(Entry.size())
				entry = Entry.from_bytes(data)
				if entry.key == key:
					return entry
			return None

def insert_entry(key:int, name:str, age:int):
	data_base = DataBase(FILE_PATH)
	insert_result = data_base.add_entry(key, name, age)
	if insert_result == OpStatus.OK:
		print('insercao com sucesso: {}'.format(key))
	elif insert_result == OpStatus.ERR_KEY_EXISTS:
		print('chave ja existente: {}'.format(key))
	elif insert_result == OpStatus.ERR_OUT_OF_SPACE:
		print('insercao de chave sem sucesso - arquivo cheio: {}'.format(key))
	else:
		print('DEBUG: erro logico na insercao da chave {}'.format(key))

		
def query_entry(key:int):
	data_base = DataBase(FILE_PATH)
	entry = data_base.entry_by_key(key)
	if entry is not None:
		print(entry)
		#print('chave: {}'.format(entry.key))
		#print(entry.name)
		#print(str(entry.age))
	else:
		print('chave nao encontrada: {}'.format(key))

def remove_entry(key:int):
	data_base = DataBase(FILE_PATH)
	remove_result = data_base.delete_by_key(key)
	if remove_result == OpStatus.OK:
		print('chave removida com sucesso: {}'.format(key))
	elif remove_result == OpStatus.ERR_KEY_NOT_FOUND:	
		print('chave nao encontrada: {}'.format(key))
	else:
		print('DEBUG: erro logico na remocao da chave {}'.format(key))

def print_file():
	data_base = DataBase(FILE_PATH)
	print(data_base)

def benchmark():
	print('BENCHMARK NÃO IMPLEMENTADO')

def exit_shell():
	sys.exit()


database = DataBase(FILE_PATH)
entry = input()
while entry != 'e':
    if(entry == 'i'):
        num_reg = input()
        name_reg = input()
        age_reg = input()
        insert_entry(int(num_reg), name_reg, int(age_reg))
    elif(entry == 'c'):
        num_reg = input()
        database.entry_by_key(int(num_reg))
    elif(entry == 'r'):
        num_reg = input()
        remove_entry(int(num_reg))
    elif(entry == 'p'):
        print_file()
    elif(entry == 'm'):
        print("FALTA IMPLEMENTAR ISSO AINDA VIU!!!!!")
    entry = input()

#TESTE
os.remove(FILE_PATH)#

insert_entry(1, "Abraham Weintraub", 11)
insert_entry(0, "Roberto Carlos", 255)
insert_entry(2, "Pedro Antonhyonhi Silva Costa", 24)
insert_entry(0, "Severino Severo", 44)
insert_entry(4, "João Gabriel", 10)
insert_entry(6, "Fausto Silva", 60)
insert_entry(5, "Pablo Vilar", 19)
print_file()

remove_entry(0)
remove_entry(6)
remove_entry(6)
remove_entry(1)
print_file()

query_entry(0)
query_entry(2)
query_entry(4)
query_entry(6)
print_file()