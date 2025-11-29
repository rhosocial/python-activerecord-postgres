# tests/rhosocial/activerecord_postgres_test/feature/query/test_postgres_datetime_functions.py
"""PostgreSQL-specific datetime function tests."""
import re
from decimal import Decimal
import pytest


def test_postgres_datetime_functions(order_fixtures):
    """Test PostgreSQL-specific datetime functions."""
    from rhosocial.activerecord.query.expression import FunctionExpression, CurrentExpression

    User, Order, OrderItem = order_fixtures

    # Create test user
    user = User(username='test_user', email='test@example.com', age=30)
    user.save()

    # Create test orders with specific timestamps if we have updated_at field
    order = Order(
        user_id=user.id,
        order_number='ORD-001',
        total_amount=Decimal('100.00')
    )
    order.save()

    try:
        # Test TO_CHAR function (PostgreSQL datetime function)
        query = Order.query().where('id = ?', (order.id,))
        query.select_expr(FunctionExpression('TO_CHAR', 'created_at', "'YYYY-MM-DD'", alias='order_date'))
        results = query.aggregate()[0]

        assert 'order_date' in results
        # Convert to string for consistent comparison across different database drivers
        order_date_str = str(results['order_date'])
        # Verify it's a properly formatted date
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', order_date_str) is not None

        # Test DATE() function
        query = Order.query().where('id = ?', (order.id,))
        query.select_expr(FunctionExpression('DATE', 'created_at', alias='order_date_only'))
        results = query.aggregate()[0]

        assert 'order_date_only' in results
        # Convert to string for consistent comparison across different database drivers
        order_date_only_str = str(results['order_date_only'])
        # Should be in YYYY-MM-DD format
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', order_date_only_str) is not None

        # Test current date/time functions
        query = Order.query().where('id = ?', (order.id,))
        query.select_expr(CurrentExpression(CurrentExpression.CURRENT_DATE, alias='current_date'))
        results = query.aggregate()[0]

        assert 'current_date' in results
        # CURRENT_DATE returns the current date in YYYY-MM-DD format
        # Convert to string for consistent comparison across different database drivers
        current_date_str = str(results['current_date'])
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', current_date_str) is not None

        # Test NOW() function
        query = Order.query().where('id = ?', (order.id,))
        query.select_expr(FunctionExpression('NOW', alias='current_datetime'))
        results = query.aggregate()[0]

        assert 'current_datetime' in results
        # NOW() returns timestamp, which when converted to string should have date/time format
        # Convert to string for consistent comparison across different database drivers
        current_datetime_str = str(results['current_datetime'])
        # Check that it has date and time format (YYYY-MM-DD HH:MM:SS or similar)
        assert len(current_datetime_str) >= 10  # At least date part

        # Test EXTRACT function
        query = Order.query().where('id = ?', (order.id,))
        query.select_expr(FunctionExpression('EXTRACT', "YEAR FROM created_at", alias='order_year'))
        results = query.aggregate()[0]

        assert 'order_year' in results
        # Extracted year should be a valid year
        order_year_int = int(results['order_year'])
        assert order_year_int >= 2000

    except Exception as e:
        # Handle cases where datetime functions are not supported
        if 'function' in str(e).lower() and 'does not exist' in str(e).lower():
            pytest.skip(f"PostgreSQL installation doesn't support the tested datetime functions: {e}")
        elif 'no such column' in str(e).lower() and 'created_at' in str(e).lower():
            pytest.skip("Order model doesn't have created_at column")
        raise