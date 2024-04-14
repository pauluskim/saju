from openai import OpenAI


def print_hi(name):
    client = OpenAI()

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system",
             "content": "You are a poetic assistant, skilled in explaining complex programming concepts with creative flair."},
            {"role": "user", "content": "Compose a poem that explains the concept of recursion in programming."}
        ]
    )

    print(completion.choices[0].message)
# ChatCompletionMessage(content="In the realm of codes that intertwine,\nA concept dances, truly divine,\nRecursion whispers with a clever grace,\nA loop that calls itself, embracing space.\n\nLike a mirror reflecting an endless stream,\nRecursive functions fulfill a programmer's dream,\nThey dive deep within, layers unfolding,\nInfinite pathways, forever molding.\n\nA function that loops, not in a line,\nBut in a loop that intertwines,\nWith each call, a new journey begins,\nUnraveling mysteries, breaking all bins.\n\nThrough recursion, patterns emerge,\nSolving problems with a rhythmic surge,\nA dance of algorithms, elegant and grand,\nIn the world of code, a magical strand.\n\nSo heed the call of recursion's song,\nInfinite loops where you belong,\nLet your functions spiral and soar,\nEmbrace the beauty of coding's core.", role='assistant', function_call=None, tool_calls=None)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
