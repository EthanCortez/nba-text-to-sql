import torch
from datasets import Dataset, disable_caching
from sklearn.model_selection import train_test_split
from transformers import AutoTokenizer, AutoModelForCausalLM, Trainer, TrainingArguments, BitsAndBytesConfig
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training
import pandas as pd

disable_caching()

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

csv_file = 'actual_data1.csv'
df = pd.read_csv(csv_file)
data = df.to_dict('records')

MODEL_NAME = "meta-llama/Llama-3.1-8B-Instruct"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
tokenizer.pad_token_id = tokenizer.eos_token_id

# Load model in 4-bit — reduces VRAM from ~16GB to ~5GB
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",           # NormalFloat4, best quality for LLMs
    bnb_4bit_compute_dtype=torch.float16, # Compute in fp16 even though weights are 4-bit
    bnb_4bit_use_double_quant=True,       # Quantize the quantization constants too, saves a bit more
)

model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    quantization_config=bnb_config,
    device_map="auto"
)

# Prepare model for 4-bit training — freezes base weights, casts layer norms to fp32
model = prepare_model_for_kbit_training(model)

# LoRA — instead of training 8B params, only train ~4M adapter params
lora_config = LoraConfig(
    r=8,                      # Rank — higher = more capacity but more VRAM
    lora_alpha=16,            # Scaling factor, usually 2x rank
    target_modules=[          # Which layers to attach LoRA adapters to
        "q_proj", "k_proj", "v_proj", "o_proj",
        "gate_proj", "up_proj", "down_proj"
    ],
    lora_dropout=0.05,
    bias="none",
    task_type="CAUSAL_LM"
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()  # Should show ~1-2% of params are trainable

def format_prompt(query):
    messages = [
        {"role": "system", "content": "You are a SQL expert. Convert the user's natural language question into a valid SQL query. Output only the SQL query with no explanation."},
        {"role": "user", "content": query}
    ]
    return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

def preprocess(examples):
    prompts = [format_prompt(inp) for inp in examples['input']]
    full_texts = [p + out + tokenizer.eos_token for p, out in zip(prompts, examples['output'])]

    tokenized = tokenizer(
        full_texts,
        max_length=512,
        truncation=True,
        padding='max_length',
        return_tensors=None
    )

    prompt_tokenized = tokenizer(
        prompts,
        padding=False,
        truncation=True,
        max_length=512,
        return_tensors=None
    )

    labels = []
    for i in range(len(examples['input'])):
        prompt_len = len(prompt_tokenized['input_ids'][i])
        label_seq = tokenized['input_ids'][i].copy()
        for j in range(len(label_seq)):
            if j < prompt_len or label_seq[j] == tokenizer.pad_token_id:
                label_seq[j] = -100
        labels.append(label_seq)

    tokenized['labels'] = labels
    return tokenized

def query_to_sql(query):
    prompt = format_prompt(query)
    inputs = tokenizer(prompt, return_tensors='pt').to(model.device)
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=256,
            num_beams=5,
            repetition_penalty=1.5,
            pad_token_id=tokenizer.eos_token_id,
        )
    new_tokens = outputs[0][inputs['input_ids'].shape[1]:]
    sql = tokenizer.decode(new_tokens, skip_special_tokens=True)
    return sql.strip()

train_data, temp_data = train_test_split(data, test_size=0.2, random_state=20)
val_data, test_data = train_test_split(temp_data, test_size=0.5, random_state=20)

print(f"Training examples: {len(train_data)}")
print(f"Validation examples: {len(val_data)}")
print(f"Test examples: {len(test_data)}")

train_dataset = Dataset.from_list(train_data).map(preprocess, batched=True)
val_dataset   = Dataset.from_list(val_data).map(preprocess,   batched=True)
test_dataset  = Dataset.from_list(test_data).map(preprocess,  batched=True)

training_args = TrainingArguments(
    output_dir='./llama_results',
    num_train_epochs=5,
    per_device_train_batch_size=1,
    per_device_eval_batch_size=1,
    gradient_accumulation_steps=8,
    learning_rate=2e-4,               # Higher LR suits LoRA — only adapters are trained
    weight_decay=0.01,
    logging_steps=20,
    eval_strategy="epoch",
    save_strategy="epoch",
    save_total_limit=2,
    fp16=True,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    remove_unused_columns=False,
    report_to="none",
    optim="paged_adamw_8bit",         # 8-bit optimizer — saves ~2GB of VRAM vs regular AdamW
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
)

trainer.train()

trainer.save_model("advanced_model")
tokenizer.save_pretrained("advanced_model")

print("\nEvaluation:")
correct = 0
total = len(test_data)

for i, example in enumerate(test_data):
    predicted = query_to_sql(example['input'])
    match = predicted.strip() == example['output'].strip()
    if match:
        correct += 1
    print(f"\nTest {i+1}:")
    print(f"Input:     {example['input']}")
    print(f"Expected:  {example['output']}")
    print(f"Predicted: {predicted}")
    print(f"Match:     {match}")

accuracy = correct / total
print(f"\nTest Set Accuracy: {accuracy:.2%} ({correct}/{total})")