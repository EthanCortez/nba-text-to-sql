import torch
from datasets import Dataset
from sklearn.model_selection import train_test_split
from transformers import T5Tokenizer, T5ForConditionalGeneration, Trainer, TrainingArguments
# from test_data import data
import pandas as pd

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

csv_file = 'actual_data1.csv'
df = pd.read_csv(csv_file)
data = df.to_dict('records')

tokenizer = T5Tokenizer.from_pretrained('t5-small')
model = T5ForConditionalGeneration.from_pretrained('t5-small')
model.to(device)

# Preprocessing
def preprocess(examples):
    prefixed_inputs = [f"translate English to SQL: {text}" for text in examples['input']]
    
    inputs = tokenizer(prefixed_inputs, max_length=128, truncation=True, padding='max_length')
    targets = tokenizer(examples['output'], max_length=256, truncation=True, padding='max_length')
    
    labels = targets['input_ids'].copy()
    temp_labels = []

    for label_seq in labels:
        processed_seq = []
        for label in label_seq:
            if label != tokenizer.pad_token_id:
                processed_seq.append(label)
            else:
                processed_seq.append(-100)
        temp_labels.append(processed_seq)

    labels = temp_labels
    inputs['labels'] = labels
    return inputs

# Inference function
def query_to_sql(query):
    prefixed_query = f"translate English to SQL: {query}"
    inputs = tokenizer(prefixed_query, return_tensors='pt').to(device)
    outputs = model.generate(
        **inputs, 
        max_length=256,
        num_beams=5,
        repetition_penalty=1.5, # increase?
    )
    sql = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return sql

# Data into 80% train, 10% val, 10% test
train_data, temp_data = train_test_split(data, test_size=0.2, random_state=20)
val_data, test_data = train_test_split(temp_data, test_size=0.5, random_state=20)

print(f"Training examples: {len(train_data)}")
print(f"Validation examples: {len(val_data)}")
print(f"Test examples: {len(test_data)}")

# Datasets
train_dataset = Dataset.from_list(train_data).map(preprocess, batched=True)
val_dataset = Dataset.from_list(val_data).map(preprocess, batched=True)
test_dataset = Dataset.from_list(test_data).map(preprocess, batched=True)

# Training arguments
training_args = TrainingArguments(
    output_dir='./results',
    num_train_epochs=100,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    learning_rate=5e-5,
    weight_decay=0.01,
    logging_steps=20,
    eval_strategy="epoch",
    save_strategy="epoch",
    save_total_limit=3,
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
)

trainer.train()

# Evaluation
print("\n Evaluation:")
correct = 0
total = len(test_data)

for i, example in enumerate(test_data):
    predicted = query_to_sql(example['input'])
    match = predicted.strip() == example['output'].strip()
    if match:
        correct += 1
    
    print(f"\nTest {i+1}:")
    print(f"Input: {example['input']}")
    print(f"Expected: {example['output']}")
    print(f"Predicted: {predicted}")
    print(f"Match: {match}")

accuracy = correct / total
print(f"\nTest Set Accuracy: {accuracy:.2%} ({correct}/{total})")

# Free query mode
# print("Enter test queries of your own (type 'exit' to quit)")
# while True:
#     user_query = input("Query: ")
#     if user_query.lower() == 'exit':
#         break
#     predicted_sql = query_to_sql(user_query)
#     print(f"Predicted SQL: {predicted_sql}")

# trainer.save_model("baseline_model")
# tokenizer.save_pretrained("baseline_model")
