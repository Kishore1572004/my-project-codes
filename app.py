import streamlit as st
import google.generativeai as genai
import os
import time

# Configure page
st.set_page_config(page_title="Kishore Chatbot", page_icon="💬", layout="wide")

# Set up your API key
GOOGLE_API_KEY = "AIzaSyA7OQchjmlL2msZZuy5ue3hMH8wx3GdBHw"
genai.configure(api_key=GOOGLE_API_KEY)

# Setup model
model = genai.GenerativeModel(model_name="gemini-1.5-pro")

def get_gemini_response(prompt, chat_history=[]):
    """Get a response from the Gemini model with a system prompt"""
    try:
        system_prompt = """
        You are Kishore, a helpful, friendly, and knowledgeable AI assistant.
        You provide clear, concise, and accurate information.
        You're happy to help with a wide range of topics and questions.
        When you don't know something, you admit it rather than making up information.
        Your responses are conversational and engaging while remaining informative.
        """
        
        # Create a chat session
        chat = model.start_chat(history=chat_history)
        
        # Add system prompt if this is a new conversation
        if not chat_history:
            chat.send_message(system_prompt)
        
        # Implement retry logic with backoff for rate limiting
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = chat.send_message(prompt)
                return response.text
            except Exception as e:
                if "429" in str(e) and attempt < max_retries - 1:
                    # Wait with exponential backoff
                    wait_time = (2 ** attempt) * 2
                    time.sleep(wait_time)
                else:
                    # Re-raise if it's not a rate limit or we've exhausted retries
                    raise
                    
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg:
            return "I've reached my usage limit. Please try again later or check your API quota."
        else:
            return f"I'm having trouble processing your request right now. Error: {error_msg}"

# Add custom styling
st.markdown("""
<style>
    .chat-container {
        border-radius: 10px;
        margin-bottom: 20px;
        padding: 15px;
        background-color: #f5f5f5;
        max-height: 500px;
        overflow-y: auto;
    }
    .user-message {
        background-color: #2e7d32;
        color: white;
        padding: 10px 15px;
        border-radius: 15px 15px 0 15px;
        margin: 5px 0;
        max-width: 70%;
        margin-left: auto;
    }
    .bot-message {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        padding: 10px 15px;
        border-radius: 15px 15px 15px 0;
        margin: 5px 0;
        max-width: 70%;
        box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    }
    .header {
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, #4CAF50 0%, #1B5E20 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 20px;
        box-shadow: 0 4px 10px rgba(0,0,0,0.1);
    }
    .input-container {
        display: flex;
        gap: 10px;
        margin-top: 10px;
    }
    .stButton > button {
        background-color: #2e7d32;
        color: white;
        border-radius: 20px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        border: none;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .stButton > button:hover {
        background-color: #1b5e20;
        box-shadow: 0 4px 10px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)

# App title and description
st.markdown("<div class='header'><h1>💬 Kishore Chatbot</h1><p>Ask me anything</p></div>", unsafe_allow_html=True)

# Initialize chat history in session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = [
        {"role": "assistant", "content": "👋 Hello! I'm Kishore. How can I help you today?"}
    ]
if 'gemini_history' not in st.session_state:
    st.session_state.gemini_history = []

# Function to handle form submission
def handle_form_submit():
    if st.session_state.message_input and st.session_state.message_input.strip():
        user_message = st.session_state.message_input
        
        # Add user message to chat history
        st.session_state.chat_history.append({"role": "user", "content": user_message})
        
        # Format history for Gemini
        formatted_history = st.session_state.gemini_history.copy()
        
        # Get response from Gemini
        with st.spinner("Thinking..."):
            response = get_gemini_response(user_message, formatted_history)
        
        # Update Gemini history
        st.session_state.gemini_history.append({"role": "user", "parts": [user_message]})
        st.session_state.gemini_history.append({"role": "model", "parts": [response]})
        
        # Add assistant response to chat history
        st.session_state.chat_history.append({"role": "assistant", "content": response})

# Function to clear the conversation
def clear_conversation():
    st.session_state.chat_history = [
        {"role": "assistant", "content": "👋 Hello! I'm Kishore. How can I help you today?"}
    ]
    st.session_state.gemini_history = []

# Display chat messages
st.markdown("<div class='chat-container'>", unsafe_allow_html=True)
for message in st.session_state.chat_history:
    role = message["role"]
    content = message["content"]
    if role == "user":
        st.markdown(f"<div class='user-message'>{content}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='bot-message'>{content}</div>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# Chat input using a form with a different key for the input field
with st.form(key="message_form", clear_on_submit=True):
    col1, col2 = st.columns([6, 1])
    with col1:
        user_input = st.text_input(
            "Type your message:", 
            key="message_input", 
            placeholder="Ask me anything..."
        )
    with col2:
        submit_button = st.form_submit_button("Send", use_container_width=True, on_click=handle_form_submit)

# Sidebar with options
with st.sidebar:
    st.header("Options")
    
    if st.button("Clear Conversation"):
        clear_conversation()
    
    st.divider()
    st.markdown("### About")
    st.markdown("""
    - Ask questions
    - Get information
    - Have a conversation
    """)
    st.divider()
    st.markdown("Powered by Kishore")
    
    st.divider()
    st.markdown("### API Key")
    st.write("To use this app, you'll need to obtain an API key from Google. Please contact the developer for more information.")