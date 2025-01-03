#include "random.hh"

namespace smith
{
    std::mt19937_64 rng;
}

int d6()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 6);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 6, 1);
}

int d9()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 9);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 9, 1);
}

int d12()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 12);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 12, 1);
}

int d20()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 20);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 20, 1);
}

int d42()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 42);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 42, 2);
}

int d100()
{
    if (file_random_machine::using_file == NULL)
    {
        static std::uniform_int_distribution<> pick(1, 100);
        return pick(smith::rng);
    }
    else
        return file_random_machine::using_file->get_random_num(1, 100, 2);
}

// random in range 1 - x
int dx(int x)
{
    if (file_random_machine::using_file == NULL)
    {
        std::uniform_int_distribution<> pick(1, x);
        return pick(smith::rng);
    }
    else
    {
        if (x == 1)
            return 1;
        int bytenum;
        if (x <= (0xff >> 3))
            bytenum = 1;
        else if (x <= (0xffff >> 3))
            bytenum = 2;
        else if (x <= (0xffffff >> 3))
            bytenum = 3;
        else
            bytenum = 4;
        return file_random_machine::using_file->get_random_num(1, x, bytenum);
    }
}

std::string random_identifier_generate()
{
    unsigned int rand_value = (dx(0xffffff) << 8) + dx(0xff); // 5 bytes
    std::string name;
    while (rand_value != 0)
    {
        unsigned int choice = (rand_value & 0x3f) % 37 + 1;
        rand_value = rand_value >> 6;

        // 1-37
        if (choice <= 26) // 1 - 26
            name.push_back('a' + choice - 1);
        else if (choice <= 27) // 27
            name.push_back('_');
        else if (choice <= 37) // 28 - 37
            name.push_back('0' + choice - 28);
    }
    return name;
}

file_random_machine::file_random_machine(string s)
    : filename(s)
{
    ifstream fin(filename, std::ios::binary);
    fin.seekg(0, std::ios::end);
    end_pos = fin.tellg();
    fin.seekg(0, std::ios::beg);

    if (end_pos == 0)
    {
        buffer = NULL;
        return;
    }

    if (end_pos < 100)
    {
        std::cerr << "Exit: rand file is too small (should larger than 100 byte)" << endl;
        exit(0);
    }
    buffer = new char[end_pos + 5];
    cur_pos = 0;
    read_byte = 0;

    fin.read(buffer, end_pos);
    fin.close();
}

file_random_machine::~file_random_machine()
{
    if (buffer != NULL)
        delete[] buffer;
}

map<string, struct file_random_machine *> file_random_machine::stream_map;
struct file_random_machine *file_random_machine::using_file = NULL;

struct file_random_machine *file_random_machine::get(string filename)
{
    if (stream_map.count(filename))
        return stream_map[filename];
    else
        return stream_map[filename] = new file_random_machine(filename);
}

bool file_random_machine::map_empty()
{
    return stream_map.empty();
}

void file_random_machine::use_file(string filename)
{
    using_file = get(filename);
}

int file_random_machine::get_random_num(int min, int max, int byte_num)
{
    if (buffer == NULL)
        return 0;

    auto scope = max - min;
    if (scope <= 0)
        return min;

    // default: small endian
    auto readable = end_pos - cur_pos;
    auto read_num = readable < byte_num ? readable : byte_num;
    int rand_num = 0;
    memcpy(&rand_num, buffer + cur_pos, read_num);
    cur_pos += read_num;

    if (cur_pos >= end_pos)
        cur_pos = 0;

    read_byte += read_num;

    return min + rand_num % scope;
}