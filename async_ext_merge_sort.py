import glob
import uuid  # for temp filename generation
import os
import asyncio

def sort_each_input_file_into_temp_files(input_file_list):
    sorted_input_files = []
    for filename in input_file_list:
        sorted_input_files.append(sort_input_file_and_get_sorted_filename(filename))
    return sorted_input_files

def sort_input_file_and_get_sorted_filename(filename):   
    numbers = read_numbers_from_file(filename)
    numbers.sort()
    sorted_filename = create_temp_filename()
    write_numbers_to_file(sorted_filename, numbers)
    return sorted_filename

def write_numbers_to_file(filename, numbers):
    with open(filename, "w") as o:
        lines = list(map(num_to_str_with_newline, numbers))
        o.writelines(lines)

def num_to_str_with_newline(number):
    return str(number) + "\n"

def read_numbers_from_file(filename):
    with open(filename, "r") as file:
        lines = file.readlines()
        numbers = []
        for line in lines:
            numbers.append(int(line.strip()))
        return numbers

def merge(list1, list2):
    i = 0
    j = 0
    merged_sorted_list = []
    while (i < len(list1) and j < len(list2)):
        if (list1[i] < list2[j]):
            merged_sorted_list.append(list1[i])
            i += 1
        else:
            merged_sorted_list.append(list2[j])
            j += 1
    while (i < len(list1)):
        merged_sorted_list.append(list1[i])
        i += 1
    while (j < len(list2)):
        merged_sorted_list.append(list2[j])
        j += 1
    return merged_sorted_list 

def sort_two_sorted_files_into_one(filename1, filename2):   
    sorted_numbers1 = read_numbers_from_file(filename1)
    sorted_numbers2 = read_numbers_from_file(filename2)
    merged_numbers = merge(sorted_numbers1, sorted_numbers2)
    merged_filename = create_temp_filename()
    write_numbers_to_file(merged_filename, merged_numbers)
    return merged_filename

def create_temp_filename():
    return str(uuid.uuid4())

def delete_temp_file(filename):
    os.remove(filename)

class CounterHolder:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.i = 0
    async def increment(self, value = 1):
        async with self.lock:
            self.i += value
    async def decrement(self):
        async with self.lock:
            print("task count decremented")
            self.i -= 1
    async def get(self):
        async with self.lock:
            return self.i

async def dispatch_merging_tasks(current_sorted_sublists, current_sorted_sublists_lock, merge_done_sema, task_counter):
    created_tasks = 0
    sublists = await list_len_lock(current_sorted_sublists, current_sorted_sublists_lock)
    while sublists > 1:
        filename1, filename2 = await get_two_files_lock(current_sorted_sublists, current_sorted_sublists_lock)
        asyncio.create_task(merge_two_lists_and_write_results(current_sorted_sublists, filename1, filename2, current_sorted_sublists_lock, merge_done_sema, task_counter))
        created_tasks += 1
        sublists = await list_len_lock(current_sorted_sublists, current_sorted_sublists_lock)
    return created_tasks

async def sort(input_file_list):
    current_sorted_sublists = sort_each_input_file_into_temp_files(input_file_list)
    current_sorted_sublists_lock = asyncio.Lock()
    merge_done_sema = asyncio.Semaphore(0)
    num_active_tasks = CounterHolder()
    await num_active_tasks.increment(await dispatch_merging_tasks(current_sorted_sublists, current_sorted_sublists_lock, merge_done_sema, num_active_tasks))
    tasks = await num_active_tasks.get()
    sublists = await list_len_lock(current_sorted_sublists, current_sorted_sublists_lock)
    while tasks > 0 or sublists > 1:
        await merge_done_sema.acquire()
        
        await num_active_tasks.increment(await dispatch_merging_tasks(current_sorted_sublists, current_sorted_sublists_lock, merge_done_event, num_active_tasks))
        tasks = await num_active_tasks.get()
        sublists = await list_len_lock(current_sorted_sublists, current_sorted_sublists_lock)
        
    print(tasks)
    print(sublists)
    save_final_results(current_sorted_sublists.pop())


async def merge_two_lists_and_write_results(current_sorted_sublists, filename1, filename2, lock, merge_done_sema, task_counter):
    new_merged_file = sort_two_sorted_files_into_one(filename1, filename2)
    delete_temp_file(filename1)
    delete_temp_file(filename2)
    await add_merged_results_to_list_lock(current_sorted_sublists, new_merged_file, lock)
    
    await task_counter.decrement()
    merge_done_sema.release()

async def list_len_lock(my_list, lock):
    async with lock:
        return len(my_list)

async def get_two_files_lock(files, lock):
    async with lock:
        filename1 = files.pop()
        filename2 = files.pop()
        return filename1, filename2

async def add_merged_results_to_list_lock(current_sorted_sublists, filename, lock):
    async with lock:
        print("added results to list")
        current_sorted_sublists.append(filename)

async def save_final_results(final_results_filename):
    final_numbers = read_numbers_from_file(final_results_filename)
    delete_temp_file(final_results_filename)
    write_numbers_to_file("sorted.txt", final_numbers)

# https://stackoverflow.com/questions/18262293/how-to-open-every-file-in-a-folder
input_files = glob.glob("input/unsorted_*.txt")
asyncio.run(sort(input_files))