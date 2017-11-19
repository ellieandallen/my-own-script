# -*- coding: utf-8 -*-
import random


def skipgrams(pages, max_context):
    """根据skip-gram模型形成训练对."""
    for words in pages:
        for index, current in enumerate(words):
            context = random.randint(1, max_context)
            for target in words[max(0, index - context): index]:
                yield current, target
            for target in words[index + 1: index + context + 1]:
                yield current, target
