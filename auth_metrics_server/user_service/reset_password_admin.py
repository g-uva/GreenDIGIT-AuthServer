# reset_password_admin.py
import sys, argparse, os
from passlib.context import CryptContext
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from login_server import User  # uses same model

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
PASSWORD_RESET_MARKER = "!RESET_REQUIRED!"  # optional: first-login on next /login

engine = create_engine("sqlite:///./users.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

"""
Usage examples:
# Set a new password immediately
python reset_password_admin.py alice@example.org --set "MyNewSecret123"

# Delete user entry (next login will trigger first-login flow)
python reset_password_admin.py alice@example.org --delete

# Mark user for reset (keeps row but forces password set at next login)
python reset_password_admin.py alice@example.org --mark-reset
"""
def main():
    p = argparse.ArgumentParser()
    p.add_argument("email")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--set", dest="new_password", help="Set a new password now")
    g.add_argument("--delete", action="store_true", help="Delete the user row (first-login flow will recreate)")
    g.add_argument("--mark-reset", action="store_true", help="Mark for first-login (next /login sets new password)")
    args = p.parse_args()

    email = args.email.strip().lower()
    db = SessionLocal()

    if args.delete:
        n = db.query(User).filter(User.email == email).delete()
        db.commit()
        print(f"Deleted {n} user(s).")
        return

    user = db.query(User).filter(User.email == email).first()
    if not user:
        print(f"User with email {email} not found.")
        sys.exit(1)

    if args.new_password:
        user.hashed_password = pwd_context.hash(args.new_password)
        db.commit()
        print(f"Password for {email} updated successfully.")
    elif args.mark_reset:
        user.hashed_password = PASSWORD_RESET_MARKER
        db.commit()
        print(f"User {email} marked to set a new password on next login.")

if __name__ == "__main__":
    main()
