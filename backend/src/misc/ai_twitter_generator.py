import os
from dotenv import load_dotenv
import openai
import json
import yaml
from pathlib import Path

# Load environment variables
load_dotenv()

class TwitterPostGenerator:
    def __init__(self):
        """Initialize the Twitter post generator with OpenAI API"""
        self.client = openai.OpenAI(
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Load prompts and examples from external files
        self.prompts_dir = Path(__file__).parent / "prompts"
        self._load_prompts_and_examples()
        
    def _load_prompts_and_examples(self):
        """Load prompts and examples from YAML files"""
        try:
            # Load system prompt from YAML
            with open(self.prompts_dir / "system_prompt.yml", 'r', encoding='utf-8') as f:
                system_config = yaml.safe_load(f)
                self.system_prompt = self._build_system_prompt(system_config['system_prompt'])
            
            # Load user prompt template from YAML
            with open(self.prompts_dir / "user_prompt.yml", 'r', encoding='utf-8') as f:
                user_config = yaml.safe_load(f)
                self.user_prompt_template = self._build_user_prompt_template(user_config)
                self.example_posts = user_config['example_posts']
                
            print("✅ Successfully loaded prompts and examples from YAML files")
            
        except FileNotFoundError as e:
            print(f"❌ Error loading YAML prompt files: {e}")
            print("📝 Using fallback hardcoded prompts...")
            self._load_fallback_prompts()
        except yaml.YAMLError as e:
            print(f"❌ Error parsing YAML files: {e}")
            print("📝 Using fallback hardcoded prompts...")
            self._load_fallback_prompts()
    
    def _build_system_prompt(self, config):
        """Build system prompt from YAML configuration"""
        prompt_parts = []
        
        # Add role and objective
        prompt_parts.append(config['role'])
        prompt_parts.append(f"\nYour role is to: {config['objective']}")
        
        # Add style and tone section
        style = config['style_and_tone']
        prompt_parts.append(f"\nSTYLE & TONE:")
        prompt_parts.append(f"- {style['overall']}")
        prompt_parts.append(f"- {style['approach']}")
        
        # Handle language guidelines if present
        if 'language' in style:
            prompt_parts.append(f"- {style['language']}")
        if 'spelling' in style:
            prompt_parts.append(f"- {style['spelling']}")
        if 'punctuation' in style:
            prompt_parts.append(f"- {style['punctuation']}")
        
        # Add emoji rules (new structure)
        if 'emoji_rules' in config:
            emoji_rules = config['emoji_rules']
            prompt_parts.append(f"- {emoji_rules['strict_usage']}")
            allowed_emojis = [emoji.split(' #')[0] for emoji in emoji_rules['allowed_emojis']]  # Remove comments
            prompt_parts.append(f"- Allowed emojis ONLY: {' '.join(allowed_emojis)}")
            prompt_parts.append(f"- {emoji_rules['forbidden']}")
        # Fallback to old structure
        elif 'emojis' in style:
            prompt_parts.append(f"- Uses relevant emojis strategically ({' '.join(style['emojis'])})")
        
        # Handle calls to action if present
        if 'calls_to_action' in style:
            prompt_parts.append(f"- {style['calls_to_action']}")
        
        # Add hashtag policy (new structure)
        if 'hashtag_policy' in config:
            hashtag_policy = config['hashtag_policy']
            prompt_parts.append(f"- Hashtag policy: {hashtag_policy['rule']}")
            prompt_parts.append(f"- Reason: {hashtag_policy['reason']}")
        # Fallback to old structure
        elif 'hashtag_strategy' in config:
            hashtags = config['hashtag_strategy']
            prompt_parts.append(f"- Hashtag strategy: {' '.join(hashtags['primary'])}")
        
        # Add content approach
        prompt_parts.append(f"\nCONTENT APPROACH:")
        for approach in config['content_approach']:
            prompt_parts.append(f"- {approach}")
        
        # Add post structure
        prompt_parts.append(f"\nPOST STRUCTURE:")
        for num, structure in config['post_structure'].items():
            prompt_parts.append(f"{num}. {structure}")
        
        # Add critical requirements
        reqs = config['critical_requirements']
        prompt_parts.append(f"\nCRITICAL REQUIREMENTS:")
        prompt_parts.append(f"- MUST stay under {reqs['character_limit']} characters ({reqs['character_limit_importance']})")
        
        # Handle structure compliance if present (new structure)
        if 'structure_compliance' in reqs:
            prompt_parts.append(f"- {reqs['structure_compliance']}")
        
        # Add data requirements
        if 'data_requirements' in reqs:
            for req in reqs['data_requirements']:
                prompt_parts.append(f"- {req}")
        
        # Add chain knowledge (new structure)
        if 'chain_knowledge' in config:
            chain_knowledge = config['chain_knowledge']
            prompt_parts.append(f"\nCHAIN KNOWLEDGE:")
            if 'layer2_chains' in chain_knowledge:
                prompt_parts.append(f"- Layer 2 chains: {chain_knowledge['layer2_chains']}")
            if 'ethereum_context' in chain_knowledge:
                prompt_parts.append(f"- Context: {chain_knowledge['ethereum_context']}")
            if 'social_handles' in chain_knowledge:
                prompt_parts.append(f"- Social handles: {chain_knowledge['social_handles']}")
            if 'data_interval' in chain_knowledge:
                prompt_parts.append(f"- Data intervals: {chain_knowledge['data_interval']}")
        
        # Add metric understanding (new structure)
        if 'metric_understanding' in config:
            metric_understanding = config['metric_understanding']
            prompt_parts.append(f"\nMETRIC UNDERSTANDING:")
            if 'abbreviations' in metric_understanding:
                for abbrev in metric_understanding['abbreviations']:
                    prompt_parts.append(f"- {abbrev}")
            if 'explanations' in metric_understanding:
                prompt_parts.append(f"- {metric_understanding['explanations']}")
        
        # Fallback: Add topics to cover (old structure)
        if 'topics_to_cover' in config:
            prompt_parts.append(f"\nTOPICS TO COVER:")
            for topic in config['topics_to_cover']:
                prompt_parts.append(f"- {topic}")
        
        # Add final instruction
        prompt_parts.append(f"\n{config['final_instruction']}")
        
        return '\n'.join(prompt_parts)
    
    def _build_user_prompt_template(self, config):
        """Build user prompt template from YAML configuration"""
        user_prompt = config['user_prompt']
        prompt_parts = []
        
        # Add template
        prompt_parts.append(user_prompt['template'])
        
        # Add requirements
        prompt_parts.append("Requirements:")
        reqs = user_prompt['requirements']
        prompt_parts.append(f"- MUST stay under {reqs['character_limit']} characters ({reqs['character_limit_importance']})")
        
        # Add other requirements (new flat structure)
        for key, value in reqs.items():
            if key not in ['character_limit', 'character_limit_importance']:
                if isinstance(value, str):
                    prompt_parts.append(f"- {value}")
        
        # Add content structure (separate section in new format)
        if 'content_structure' in user_prompt:
            prompt_parts.append("\nContent Structure:")
            for structure_item in user_prompt['content_structure']:
                prompt_parts.append(f"- {structure_item}")
        
        # Add style inspiration text
        prompt_parts.append(f"\n{user_prompt['style_inspiration_text']}")
        
        # Add final instruction
        prompt_parts.append(f"\n{user_prompt['final_instruction']}")
        
        return '\n'.join(prompt_parts)
        
    def _load_fallback_prompts(self):
        """Fallback prompts in case external files can't be loaded"""
        self.system_prompt = """You are an expert social media manager for growthepie.xyz. Create engaging, data-driven Twitter posts with emojis and hashtags."""
        
        self.user_prompt_template = """Create a Twitter post based on this data: {data_input}
        
        Requirements: Use specific metrics, include emojis, end with hashtags.
        
        Example style: {example_posts}"""
        
        self.example_posts = [
            "🚀 Layer 2 growth is incredible! 📊 #Layer2 #Ethereum",
            "📈 TVL hitting new highs! 💰 #DeFi #Analytics"
        ]
        
    def get_system_prompt(self):
        """Get the system prompt"""
        return self.system_prompt

    def get_user_prompt(self, data_input):
        """Create user prompt based on input data using template"""
        return self.user_prompt_template.format(
            data_input=data_input,
            example_posts=json.dumps(self.example_posts, indent=2)
        )

    def generate_tweet(self, data_input, temperature=0.7):
        """Generate a Twitter post using OpenAI"""
        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": self.get_system_prompt()},
                    {"role": "user", "content": self.get_user_prompt(data_input)}
                ],
                temperature=temperature,
                max_tokens=300
            )
            
            tweet = response.choices[0].message.content.strip()
            return tweet
            
        except Exception as e:
            print(f"Error generating tweet: {e}")
            return None

    def generate_multiple_variants(self, data_input, num_variants=3):
        """Generate multiple tweet variants for A/B testing"""
        variants = []
        temperatures = [0.5, 0.7, 0.9]  # Different creativity levels
        
        for i, temp in enumerate(temperatures[:num_variants]):
            print(f"Generating variant {i+1}/{num_variants}...")
            tweet = self.generate_tweet(data_input, temperature=temp)
            if tweet:
                variants.append({
                    "variant": i+1,
                    "temperature": temp,
                    "tweet": tweet,
                    "character_count": len(tweet)
                })
        
        return variants

    def reload_prompts(self):
        """Reload prompts from external files (useful for testing prompt changes)"""
        print("🔄 Reloading prompts from external files...")
        self._load_prompts_and_examples()
        print("✅ Prompts reloaded successfully!")

# Example usage and testing
if __name__ == "__main__":
    generator = TwitterPostGenerator()
    
    test_data = """
    Layer 2 Analytics Update:
    - Base TVL: $5.2B (+15% this week)
    - Arbitrum transactions: 2.1M daily
    - zkSync active users: 450K
    """
    
    print("🧪 Testing Twitter generator...")
    tweets = generator.generate_multiple_variants(test_data, num_variants=2)
    
    for tweet in tweets:
        print(f"\n--- Tweet Variant {tweet['variant']} ---")
        print(f"Temperature: {tweet['temperature']}")
        print(f"Characters: {tweet['character_count']}/280")
        print(tweet['tweet'])

